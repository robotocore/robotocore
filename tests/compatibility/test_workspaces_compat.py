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


class TestWorkSpacesOperations:
    """Tests for core WorkSpaces operations."""

    def test_describe_workspaces_empty(self, workspaces):
        """Describing workspaces returns an empty list when none exist."""
        result = workspaces.describe_workspaces()
        assert "Workspaces" in result
        assert isinstance(result["Workspaces"], list)

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

    def test_describe_workspace_images_empty(self, workspaces):
        """Describing workspace images returns an empty list when none exist."""
        result = workspaces.describe_workspace_images()
        assert "Images" in result
        assert isinstance(result["Images"], list)


class TestWorkSpacesImagePermissions:
    """Test DescribeWorkspaceImagePermissions."""

    def test_describe_workspace_image_permissions_nonexistent(self, workspaces):
        """DescribeWorkspaceImagePermissions for nonexistent image returns error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_workspace_image_permissions(ImageId="wsi-nonexistent123")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "AccessDeniedException",
            "ValidationException",
        )


class TestWorkSpacesFiltering:
    """Tests for WorkSpaces filtering and query operations."""

    def test_describe_workspaces_by_workspace_ids_nonexistent(self, workspaces):
        """Filtering by nonexistent WorkspaceIds returns empty list."""
        result = workspaces.describe_workspaces(WorkspaceIds=["ws-nonexistent123"])
        assert "Workspaces" in result
        assert result["Workspaces"] == []

    def test_describe_workspaces_by_bundle_id_nonexistent(self, workspaces):
        """Filtering by nonexistent BundleId returns empty list."""
        result = workspaces.describe_workspaces(BundleId="wsb-nonexistent123")
        assert "Workspaces" in result
        assert result["Workspaces"] == []

    def test_describe_workspaces_by_directory_and_username_nonexistent(self, workspaces):
        """Filtering by nonexistent DirectoryId+UserName returns empty list."""
        result = workspaces.describe_workspaces(DirectoryId="d-nonexistent1234", UserName="nobody")
        assert "Workspaces" in result
        assert result["Workspaces"] == []

    def test_describe_workspace_images_by_ids_nonexistent(self, workspaces):
        """Filtering images by nonexistent ImageIds returns empty list."""
        result = workspaces.describe_workspace_images(ImageIds=["wsi-nonexistent123"])
        assert "Images" in result
        assert result["Images"] == []


class TestWorkSpacesClientProperties:
    """Tests for DescribeClientProperties."""

    def test_describe_client_properties_empty(self, workspaces):
        """DescribeClientProperties with a nonexistent resource returns empty list."""
        result = workspaces.describe_client_properties(ResourceIds=["d-9267462133"])
        assert "ClientPropertiesList" in result
        assert isinstance(result["ClientPropertiesList"], list)


class TestWorkSpacesImageOperations:
    """Tests for WorkSpaces image operations."""

    def test_create_workspace_image_nonexistent_workspace(self, workspaces):
        """CreateWorkspaceImage with a nonexistent workspace raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.create_workspace_image(
                Name=_unique("img"),
                Description="test image",
                WorkspaceId="ws-nonexistent123",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesTermination:
    """Tests for workspace termination edge cases."""

    def test_terminate_nonexistent_workspace(self, workspaces):
        """Terminating a nonexistent workspace returns a FailedRequests entry."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-nonexistent123"
        assert result["FailedRequests"][0]["ErrorCode"] == "400"


class TestWorkSpacesDirectoryValidation:
    """Tests for directory-related validation."""

    def test_describe_directories_with_invalid_id_format(self, workspaces):
        """DescribeWorkspaceDirectories with invalid directory ID raises ValidationException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_workspace_directories(DirectoryIds=["d-nonexistent1234"])
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_modify_workspace_creation_properties_invalid_directory(self, workspaces):
        """ModifyWorkspaceCreationProperties with invalid directory raises ValidationException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_creation_properties(
                ResourceId="d-0000000000",
                WorkspaceCreationProperties={"EnableInternetAccess": True},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestWorkSpacesAccountOperations:
    """Tests for WorkSpaces account-level operations."""

    def test_describe_account(self, workspaces):
        """DescribeAccount returns account info."""
        resp = workspaces.describe_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DedicatedTenancySupport" in resp or "DedicatedTenancyManagementCidrRange" in resp

    def test_describe_account_modifications(self, workspaces):
        """DescribeAccountModifications returns list of modifications."""
        resp = workspaces.describe_account_modifications()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountModifications" in resp
        assert isinstance(resp["AccountModifications"], list)


class TestWorkSpacesBundleOperations:
    """Tests for WorkSpaces bundle operations."""

    def test_describe_workspace_bundles(self, workspaces):
        """DescribeWorkspaceBundles returns available bundles."""
        resp = workspaces.describe_workspace_bundles()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Bundles" in resp
        assert isinstance(resp["Bundles"], list)


class TestWorkSpacesIpGroupOperations:
    """Tests for WorkSpaces IP group operations."""

    def test_describe_ip_groups(self, workspaces):
        """DescribeIpGroups returns IP groups (possibly empty)."""
        resp = workspaces.describe_ip_groups()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Result" in resp
        assert isinstance(resp["Result"], list)


class TestWorkSpacesConnectionAliasOperations:
    """Tests for WorkSpaces connection alias operations."""

    def test_describe_connection_aliases(self, workspaces):
        """DescribeConnectionAliases returns aliases (possibly empty)."""
        resp = workspaces.describe_connection_aliases()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ConnectionAliases" in resp
        assert isinstance(resp["ConnectionAliases"], list)


class TestWorkSpacesPoolOperations:
    """Tests for WorkSpaces pool operations."""

    def test_describe_workspaces_pools(self, workspaces):
        """DescribeWorkspacesPools returns pools (possibly empty)."""
        resp = workspaces.describe_workspaces_pools()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspacesPools" in resp
        assert isinstance(resp["WorkspacesPools"], list)


class TestWorkSpacesConnectionStatusOperations:
    """Tests for WorkSpaces connection status operations."""

    def test_describe_workspaces_connection_status(self, workspaces):
        """DescribeWorkspacesConnectionStatus returns status list."""
        resp = workspaces.describe_workspaces_connection_status()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspacesConnectionStatus" in resp
        assert isinstance(resp["WorkspacesConnectionStatus"], list)


class TestWorkSpacesAccountLinkOperations:
    """Tests for WorkSpaces account link operations."""

    def test_list_account_links(self, workspaces):
        """ListAccountLinks returns list of account links."""
        resp = workspaces.list_account_links()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountLinks" in resp
        assert isinstance(resp["AccountLinks"], list)

    def test_get_account_link_nonexistent(self, workspaces):
        """GetAccountLink for nonexistent link raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.get_account_link(LinkId="al-nonexistent12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesManagementCidrOperations:
    """Tests for WorkSpaces management CIDR operations."""

    def test_list_available_management_cidr_ranges(self, workspaces):
        """ListAvailableManagementCidrRanges returns CIDR ranges."""
        resp = workspaces.list_available_management_cidr_ranges(
            ManagementCidrRangeConstraint="10.0.0.0/8"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ManagementCidrRanges" in resp
        assert isinstance(resp["ManagementCidrRanges"], list)


class TestWorkSpacesApplicationOperations:
    """Tests for WorkSpaces application operations."""

    def test_describe_applications(self, workspaces):
        """DescribeApplications returns list of applications."""
        resp = workspaces.describe_applications()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Applications" in resp
        assert isinstance(resp["Applications"], list)

    def test_describe_application_associations(self, workspaces):
        """DescribeApplicationAssociations returns associations for an app."""
        resp = workspaces.describe_application_associations(
            ApplicationId="wsa-fake12345",
            AssociatedResourceTypes=["WORKSPACE"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)


class TestWorkSpacesAssociationOperations:
    """Tests for WorkSpaces association operations."""

    def test_describe_bundle_associations(self, workspaces):
        """DescribeBundleAssociations returns associations for a bundle."""
        resp = workspaces.describe_bundle_associations(
            BundleId="wsb-fake12345",
            AssociatedResourceTypes=["APPLICATION"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)

    def test_describe_image_associations(self, workspaces):
        """DescribeImageAssociations returns associations for an image."""
        resp = workspaces.describe_image_associations(
            ImageId="wsi-fake12345",
            AssociatedResourceTypes=["APPLICATION"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)

    def test_describe_workspace_associations(self, workspaces):
        """DescribeWorkspaceAssociations returns associations for a workspace."""
        resp = workspaces.describe_workspace_associations(
            WorkspaceId="ws-fake12345",
            AssociatedResourceTypes=["APPLICATION"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)


class TestWorkSpacesConnectClientAddIns:
    """Tests for WorkSpaces Connect Client Add-Ins operations."""

    def test_describe_connect_client_add_ins(self, workspaces):
        """DescribeConnectClientAddIns returns add-ins for a directory."""
        resp = workspaces.describe_connect_client_add_ins(ResourceId="d-0000000000")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AddIns" in resp
        assert isinstance(resp["AddIns"], list)


class TestWorkSpacesSnapshotOperations:
    """Tests for WorkSpaces snapshot operations."""

    def test_describe_workspace_snapshots_nonexistent(self, workspaces):
        """DescribeWorkspaceSnapshots for nonexistent workspace raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_workspace_snapshots(WorkspaceId="ws-fake12345abc")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesPoolSessionOperations:
    """Tests for WorkSpaces pool session operations."""

    def test_describe_workspaces_pool_sessions_nonexistent(self, workspaces):
        """DescribeWorkspacesPoolSessions for nonexistent pool raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_workspaces_pool_sessions(PoolId="wspool-fake12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesClientBrandingOperations:
    """Tests for WorkSpaces client branding operations."""

    def test_describe_client_branding_nonexistent(self, workspaces):
        """DescribeClientBranding for nonexistent directory raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_client_branding(ResourceId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesConnectionAliasPermissionOperations:
    """Tests for WorkSpaces connection alias permission operations."""

    def test_describe_connection_alias_permissions_nonexistent(self, workspaces):
        """DescribeConnectionAliasPermissions raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_connection_alias_permissions(AliasId="wsca-fake12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesCustomImageImportOperations:
    """Tests for WorkSpaces custom workspace image import operations."""

    def test_describe_custom_workspace_image_import_nonexistent(self, workspaces):
        """DescribeCustomWorkspaceImageImport raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_custom_workspace_image_import(ImageId="wsi-fake12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
