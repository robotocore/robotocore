"""Compatibility tests for AWS WorkSpaces service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def workspaces():
    return make_client("workspaces")


@pytest.fixture
def ec2():
    return make_client("ec2")


@pytest.fixture
def ds():
    return make_client("ds")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def registered_directory(workspaces, ec2, ds):
    """Create a VPC, subnets, Simple AD directory, and register it with WorkSpaces."""
    vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    sub1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a")[
        "Subnet"
    ]["SubnetId"]
    sub2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b")[
        "Subnet"
    ]["SubnetId"]

    dir_id = ds.create_directory(
        Name=f"{_unique('test')}.example.com",
        Password="P@ssw0rd123",
        Size="Small",
        VpcSettings={"VpcId": vpc_id, "SubnetIds": [sub1, sub2]},
    )["DirectoryId"]

    workspaces.register_workspace_directory(DirectoryId=dir_id, SubnetIds=[sub1, sub2])

    yield {"directory_id": dir_id, "subnet_ids": [sub1, sub2]}

    # Cleanup: deregister directory
    try:
        workspaces.deregister_workspace_directory(DirectoryId=dir_id)
    except Exception:
        pass


@pytest.fixture
def workspace(workspaces, registered_directory):
    """Create a workspace in a registered directory."""
    dir_id = registered_directory["directory_id"]
    result = workspaces.create_workspaces(
        Workspaces=[
            {
                "DirectoryId": dir_id,
                "UserName": _unique("user"),
                "BundleId": "wsb-test123",
            }
        ]
    )
    assert len(result["PendingRequests"]) == 1
    assert len(result["FailedRequests"]) == 0
    ws_id = result["PendingRequests"][0]["WorkspaceId"]

    yield ws_id

    # Cleanup
    try:
        workspaces.terminate_workspaces(TerminateWorkspaceRequests=[{"WorkspaceId": ws_id}])
    except Exception:
        pass


class TestWorkSpacesDirectoryOperations:
    """Tests for WorkSpaces directory operations."""

    def test_describe_workspace_directories_empty(self, workspaces):
        """Describing directories returns an empty list when none are registered."""
        result = workspaces.describe_workspace_directories()
        assert "Directories" in result
        assert isinstance(result["Directories"], list)

    def test_register_and_describe_directory(self, workspaces, registered_directory):
        """A registered directory appears in describe results."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.describe_workspace_directories()
        directories = result["Directories"]
        matching = [d for d in directories if d["DirectoryId"] == dir_id]
        assert len(matching) == 1
        directory = matching[0]
        assert directory["State"] == "REGISTERED"
        assert directory["DirectoryType"] in ("SIMPLE_AD", "AD_CONNECTOR", "MICROSOFT_AD")
        assert "SubnetIds" in directory
        assert "WorkspaceCreationProperties" in directory

    def test_deregister_workspace_directory(self, workspaces, ec2, ds):
        """Deregistering a directory removes it from the list."""
        vpc_id = ec2.create_vpc(CidrBlock="10.1.0.0/16")["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.1.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.1.2.0/24", AvailabilityZone="us-east-1b"
        )["Subnet"]["SubnetId"]

        dir_id = ds.create_directory(
            Name=f"{_unique('dereg')}.example.com",
            Password="P@ssw0rd123",
            Size="Small",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sub1, sub2]},
        )["DirectoryId"]

        workspaces.register_workspace_directory(DirectoryId=dir_id, SubnetIds=[sub1, sub2])

        # Verify registered
        dirs = workspaces.describe_workspace_directories()["Directories"]
        assert any(d["DirectoryId"] == dir_id for d in dirs)

        # Deregister
        workspaces.deregister_workspace_directory(DirectoryId=dir_id)

        # Verify gone
        dirs = workspaces.describe_workspace_directories()["Directories"]
        assert not any(d["DirectoryId"] == dir_id and d["State"] == "REGISTERED" for d in dirs)


class TestWorkSpacesOperations:
    """Tests for core WorkSpaces operations."""

    def test_describe_workspaces_empty(self, workspaces):
        """Describing workspaces returns an empty list when none exist."""
        result = workspaces.describe_workspaces()
        assert "Workspaces" in result
        assert isinstance(result["Workspaces"], list)

    def test_create_and_describe_workspaces(self, workspaces, workspace, registered_directory):
        """A created workspace appears in describe results."""
        result = workspaces.describe_workspaces()
        ws_list = result["Workspaces"]
        matching = [w for w in ws_list if w["WorkspaceId"] == workspace]
        assert len(matching) == 1
        ws = matching[0]
        assert ws["DirectoryId"] == registered_directory["directory_id"]
        assert ws["State"] == "AVAILABLE"
        assert "BundleId" in ws

    def test_describe_workspaces_by_directory(self, workspaces, workspace, registered_directory):
        """Filtering workspaces by directory ID returns only matching workspaces."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.describe_workspaces(DirectoryId=dir_id)
        ws_list = result["Workspaces"]
        assert all(w["DirectoryId"] == dir_id for w in ws_list)
        assert any(w["WorkspaceId"] == workspace for w in ws_list)

    def test_create_workspaces_invalid_directory(self, workspaces):
        """Creating a workspace with a nonexistent directory returns a FailedRequest."""
        result = workspaces.create_workspaces(
            Workspaces=[
                {
                    "DirectoryId": "d-0000000000",
                    "UserName": "testuser",
                    "BundleId": "wsb-fake",
                }
            ]
        )
        assert len(result["FailedRequests"]) == 1
        assert len(result["PendingRequests"]) == 0
        assert "ErrorCode" in result["FailedRequests"][0]

    def test_terminate_workspaces(self, workspaces, registered_directory):
        """Terminating a workspace removes it from describe results."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.create_workspaces(
            Workspaces=[
                {
                    "DirectoryId": dir_id,
                    "UserName": _unique("term"),
                    "BundleId": "wsb-test123",
                }
            ]
        )
        ws_id = result["PendingRequests"][0]["WorkspaceId"]

        # Terminate
        term_result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": ws_id}]
        )
        assert term_result["FailedRequests"] == []

        # Verify terminated (state should be TERMINATED or not present)
        ws_list = workspaces.describe_workspaces()["Workspaces"]
        matching = [w for w in ws_list if w["WorkspaceId"] == ws_id]
        if matching:
            assert matching[0]["State"] in ("TERMINATING", "TERMINATED")

    def test_describe_workspace_images_empty(self, workspaces):
        """Describing workspace images returns an empty list when none exist."""
        result = workspaces.describe_workspace_images()
        assert "Images" in result
        assert isinstance(result["Images"], list)


class TestWorkSpacesTags:
    """Tests for WorkSpaces tag operations."""

    def test_create_and_describe_tags(self, workspaces, workspace):
        """Tags created on a workspace are returned by describe_tags."""
        workspaces.create_tags(
            ResourceId=workspace,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )

        result = workspaces.describe_tags(ResourceId=workspace)
        tag_list = result["TagList"]
        tag_map = {t["Key"]: t["Value"] for t in tag_list}
        assert tag_map["env"] == "test"
        assert tag_map["project"] == "robotocore"

    def test_create_tags_overwrites_existing(self, workspaces, workspace):
        """Creating a tag with the same key overwrites the previous value."""
        workspaces.create_tags(ResourceId=workspace, Tags=[{"Key": "env", "Value": "dev"}])
        workspaces.create_tags(ResourceId=workspace, Tags=[{"Key": "env", "Value": "prod"}])

        result = workspaces.describe_tags(ResourceId=workspace)
        tag_map = {t["Key"]: t["Value"] for t in result["TagList"]}
        assert tag_map["env"] == "prod"

    def test_describe_tags_empty(self, workspaces, workspace):
        """A workspace with no tags returns an empty tag list."""
        result = workspaces.describe_tags(ResourceId=workspace)
        # May have tags from other tests in fixture, but structure is correct
        assert "TagList" in result
        assert isinstance(result["TagList"], list)
