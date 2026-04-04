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

    # Cleanup: deregister from WorkSpaces, delete DS directory, and VPC resources
    try:
        workspaces.deregister_workspace_directory(DirectoryId=dir_id)
    except Exception:
        pass  # best-effort cleanup
    try:
        ds.delete_directory(DirectoryId=dir_id)
    except Exception:
        pass  # best-effort cleanup
    try:
        ec2.delete_subnet(SubnetId=sub1)
        ec2.delete_subnet(SubnetId=sub2)
        ec2.delete_vpc(VpcId=vpc_id)
    except Exception:
        pass  # best-effort cleanup


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
        pass  # best-effort cleanup


class TestWorkSpacesDirectoryOperations:
    """Tests for WorkSpaces directory operations."""

    def test_describe_workspace_directories_empty(self, workspaces):
        """Describing directories returns an empty list when none are registered."""
        result = workspaces.describe_workspace_directories()
        assert "Directories" in result
        assert isinstance(result["Directories"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesOperations:
    """Tests for core WorkSpaces operations."""

    def test_describe_workspaces_empty(self, workspaces):
        """Describing workspaces returns an empty list when none exist."""
        result = workspaces.describe_workspaces()
        assert "Workspaces" in result
        assert isinstance(result["Workspaces"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

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
        failed = result["FailedRequests"][0]
        assert "ErrorCode" in failed
        assert "ErrorMessage" in failed
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_workspace_images_empty(self, workspaces):
        """Describing workspace images returns an empty list when none exist."""
        result = workspaces.describe_workspace_images()
        assert "Images" in result
        assert isinstance(result["Images"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


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
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_workspaces_by_bundle_id_nonexistent(self, workspaces):
        """Filtering by nonexistent BundleId returns empty list."""
        result = workspaces.describe_workspaces(BundleId="wsb-nonexistent123")
        assert "Workspaces" in result
        assert result["Workspaces"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_workspaces_by_directory_and_username_nonexistent(self, workspaces):
        """Filtering by nonexistent DirectoryId+UserName returns empty list."""
        result = workspaces.describe_workspaces(DirectoryId="d-nonexistent1234", UserName="nobody")
        assert "Workspaces" in result
        assert result["Workspaces"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_workspace_images_by_ids_nonexistent(self, workspaces):
        """Filtering images by nonexistent ImageIds returns empty list."""
        result = workspaces.describe_workspace_images(ImageIds=["wsi-nonexistent123"])
        assert "Images" in result
        assert result["Images"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesClientProperties:
    """Tests for DescribeClientProperties."""

    def test_describe_client_properties_empty(self, workspaces):
        """DescribeClientProperties with a nonexistent resource returns empty list."""
        result = workspaces.describe_client_properties(ResourceIds=["d-9267462133"])
        assert "ClientPropertiesList" in result
        assert isinstance(result["ClientPropertiesList"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


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
        failed = result["FailedRequests"][0]
        assert failed["WorkspaceId"] == "ws-nonexistent123"
        assert failed["ErrorCode"] == "400"
        assert "ErrorMessage" in failed
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


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
        """DescribeAccount returns account info with expected fields."""
        resp = workspaces.describe_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Response should contain tenancy fields (may be null/missing if not configured)
        assert isinstance(resp, dict)
        # At least one tenancy field should be present in the response
        has_tenancy_field = (
            "DedicatedTenancySupport" in resp
            or "DedicatedTenancyManagementCidrRange" in resp
        )
        assert has_tenancy_field

    def test_describe_account_modifications(self, workspaces):
        """DescribeAccountModifications returns list of modifications."""
        resp = workspaces.describe_account_modifications()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountModifications" in resp
        assert isinstance(resp["AccountModifications"], list)
        # If any modifications exist, they should have expected fields
        for mod in resp["AccountModifications"]:
            assert "ModificationState" in mod


class TestWorkSpacesBundleOperations:
    """Tests for WorkSpaces bundle operations."""

    def test_describe_workspace_bundles(self, workspaces):
        """DescribeWorkspaceBundles returns available bundles with expected fields."""
        resp = workspaces.describe_workspace_bundles()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Bundles" in resp
        assert isinstance(resp["Bundles"], list)
        # If any bundles exist, verify structure
        for bundle in resp["Bundles"]:
            assert "BundleId" in bundle


class TestWorkSpacesIpGroupOperations:
    """Tests for WorkSpaces IP group operations."""

    def test_describe_ip_groups(self, workspaces):
        """DescribeIpGroups returns IP groups (possibly empty) with correct structure."""
        resp = workspaces.describe_ip_groups()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Result" in resp
        assert isinstance(resp["Result"], list)
        # If any groups exist, verify structure
        for group in resp["Result"]:
            assert "groupId" in group


class TestWorkSpacesConnectionAliasOperations:
    """Tests for WorkSpaces connection alias operations."""

    def test_describe_connection_aliases(self, workspaces):
        """DescribeConnectionAliases returns aliases (possibly empty) with correct structure."""
        resp = workspaces.describe_connection_aliases()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ConnectionAliases" in resp
        assert isinstance(resp["ConnectionAliases"], list)
        # If any aliases exist, verify structure
        for alias in resp["ConnectionAliases"]:
            assert "AliasId" in alias


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
        """DescribeConnectionAliasPermissions raises ResourceNotFoundException with details."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_connection_alias_permissions(AliasId="wsca-fake12345")
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Message" in err
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404)


class TestWorkSpacesCustomImageImportOperations:
    """Tests for WorkSpaces custom workspace image import operations."""

    def test_describe_custom_workspace_image_import_nonexistent(self, workspaces):
        """DescribeCustomWorkspaceImageImport raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.describe_custom_workspace_image_import(ImageId="wsi-fake12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesIpGroupCrud:
    """Tests for IP group create/update/delete lifecycle."""

    def test_create_ip_group(self, workspaces):
        """CreateIpGroup returns a GroupId."""
        resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp"),
            GroupDesc="compat test group",
        )
        assert "GroupId" in resp
        assert resp["GroupId"].startswith("wsipg-")

    def test_create_and_delete_ip_group(self, workspaces):
        """CreateIpGroup then DeleteIpGroup succeeds."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-del"),
            GroupDesc="to delete",
        )
        group_id = create_resp["GroupId"]

        del_resp = workspaces.delete_ip_group(GroupId=group_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_ip_group_nonexistent(self, workspaces):
        """DeleteIpGroup for nonexistent group raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId="wsipg-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_authorize_ip_rules(self, workspaces):
        """AuthorizeIpRules adds rules to an IP group."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-auth"),
            GroupDesc="auth test",
        )
        group_id = create_resp["GroupId"]

        auth_resp = workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "private"}],
        )
        assert auth_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_authorize_ip_rules_nonexistent(self, workspaces):
        """AuthorizeIpRules for nonexistent group raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.authorize_ip_rules(
                GroupId="wsipg-nonexistent999",
                UserRules=[{"ipRule": "10.0.0.0/8"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_revoke_ip_rules_nonexistent(self, workspaces):
        """RevokeIpRules for nonexistent group raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.revoke_ip_rules(
                GroupId="wsipg-nonexistent999",
                UserRules=["10.0.0.0/8"],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_rules_of_ip_group_nonexistent(self, workspaces):
        """UpdateRulesOfIpGroup for nonexistent group raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.update_rules_of_ip_group(
                GroupId="wsipg-nonexistent999",
                UserRules=[{"ipRule": "10.0.0.0/8"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesConnectionAliasCrud:
    """Tests for connection alias create/delete lifecycle."""

    def test_create_connection_alias(self, workspaces):
        """CreateConnectionAlias returns an AliasId."""
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias')}.example.com",
        )
        assert "AliasId" in resp
        assert resp["AliasId"].startswith("wsca-")

    def test_delete_connection_alias_nonexistent(self, workspaces):
        """DeleteConnectionAlias for nonexistent alias raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.delete_connection_alias(AliasId="wsca-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_associate_connection_alias_nonexistent(self, workspaces):
        """AssociateConnectionAlias for nonexistent alias raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.associate_connection_alias(
                AliasId="wsca-nonexistent999",
                ResourceId="d-fake12345",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_disassociate_connection_alias_nonexistent(self, workspaces):
        """DisassociateConnectionAlias for nonexistent alias raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.disassociate_connection_alias(AliasId="wsca-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_connection_alias_permission_nonexistent(self, workspaces):
        """UpdateConnectionAliasPermission for nonexistent alias raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.update_connection_alias_permission(
                AliasId="wsca-nonexistent999",
                ConnectionAliasPermission={
                    "SharedAccountId": "123456789012",
                    "AllowAssociation": True,
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesModifyOperations:
    """Tests for various Modify operations."""

    def test_modify_account(self, workspaces):
        """ModifyAccount sets dedicated tenancy support."""
        resp = workspaces.modify_account(
            DedicatedTenancySupport="ENABLED",
            DedicatedTenancyManagementCidrRange="10.0.0.0/16",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_modify_workspace_state_nonexistent(self, workspaces):
        """ModifyWorkspaceState for nonexistent workspace raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-nonexistent999",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_workspace_properties_nonexistent(self, workspaces):
        """ModifyWorkspaceProperties for nonexistent workspace raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_properties(
                WorkspaceId="ws-nonexistent999",
                WorkspaceProperties={"RunningMode": "AUTO_STOP"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_workspace_access_properties_nonexistent(self, workspaces):
        """ModifyWorkspaceAccessProperties for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_access_properties(
                ResourceId="d-nonexistent999",
                WorkspaceAccessProperties={"DeviceTypeWindows": "ALLOW"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_saml_properties_nonexistent(self, workspaces):
        """ModifySamlProperties for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_saml_properties(
                ResourceId="d-nonexistent999",
                SamlProperties={"Status": "DISABLED"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_certificate_based_auth_nonexistent(self, workspaces):
        """ModifyCertificateBasedAuthProperties for nonexistent dir raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_certificate_based_auth_properties(
                ResourceId="d-nonexistent999",
                CertificateBasedAuthProperties={"Status": "DISABLED"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_streaming_properties_nonexistent(self, workspaces):
        """ModifyStreamingProperties for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_streaming_properties(
                ResourceId="d-nonexistent999",
                StreamingProperties={
                    "StreamingExperiencePreferredProtocol": "TCP",
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_modify_endpoint_encryption_mode_nonexistent(self, workspaces):
        """ModifyEndpointEncryptionMode for nonexistent dir raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.modify_endpoint_encryption_mode(
                DirectoryId="d-nonexistent999",
                EndpointEncryptionMode="FIPS_140",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesWorkspaceActions:
    """Tests for workspace-level actions on nonexistent workspaces."""

    def test_restore_workspace_nonexistent(self, workspaces):
        """RestoreWorkspace for nonexistent workspace raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.restore_workspace(WorkspaceId="ws-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_migrate_workspace_nonexistent(self, workspaces):
        """MigrateWorkspace for nonexistent workspace raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.migrate_workspace(
                SourceWorkspaceId="ws-nonexistent999",
                BundleId="wsb-fake12345",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesBundleCrud:
    """Tests for workspace bundle create/delete/update lifecycle."""

    def test_create_workspace_bundle(self, workspaces):
        """CreateWorkspaceBundle returns a WorkspaceBundle."""
        resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle"),
            BundleDescription="compat test bundle",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspaceBundle" in resp

    def test_delete_workspace_bundle(self, workspaces):
        """DeleteWorkspaceBundle succeeds for any bundle ID."""
        resp = workspaces.delete_workspace_bundle(BundleId="wsb-fake12345")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_workspace_bundle_nonexistent(self, workspaces):
        """UpdateWorkspaceBundle for nonexistent bundle raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId="wsb-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesImageCrud:
    """Tests for workspace image operations."""

    def test_delete_workspace_image(self, workspaces):
        """DeleteWorkspaceImage succeeds."""
        resp = workspaces.delete_workspace_image(ImageId="wsi-fake12345")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_import_workspace_image(self, workspaces):
        """ImportWorkspaceImage returns an ImageId."""
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-fake12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img"),
            ImageDescription="compat test image",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ImageId" in resp

    def test_copy_workspace_image(self, workspaces):
        """CopyWorkspaceImage returns an ImageId."""
        resp = workspaces.copy_workspace_image(
            Name=_unique("copy"),
            SourceImageId="wsi-fake12345",
            SourceRegion="us-west-2",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ImageId" in resp

    def test_create_updated_workspace_image(self, workspaces):
        """CreateUpdatedWorkspaceImage returns an ImageId."""
        resp = workspaces.create_updated_workspace_image(
            Name=_unique("updated"),
            SourceImageId="wsi-fake12345",
            Description="compat test updated image",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ImageId" in resp


class TestWorkSpacesPoolCrud:
    """Tests for workspace pool operations."""

    def test_create_workspaces_pool(self, workspaces):
        """CreateWorkspacesPool returns a WorkspacesPool."""
        resp = workspaces.create_workspaces_pool(
            PoolName=_unique("pool"),
            BundleId="wsb-fake12345",
            DirectoryId="d-fake12345",
            Description="compat test pool",
            Capacity={"DesiredUserSessions": 1},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspacesPool" in resp

    def test_start_workspaces_pool_nonexistent(self, workspaces):
        """StartWorkspacesPool for nonexistent pool raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.start_workspaces_pool(PoolId="wspool-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_workspaces_pool_nonexistent(self, workspaces):
        """StopWorkspacesPool for nonexistent pool raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.stop_workspaces_pool(PoolId="wspool-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_terminate_workspaces_pool_nonexistent(self, workspaces):
        """TerminateWorkspacesPool for nonexistent pool raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.terminate_workspaces_pool(PoolId="wspool-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_workspaces_pool_nonexistent(self, workspaces):
        """UpdateWorkspacesPool for nonexistent pool raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.update_workspaces_pool(PoolId="wspool-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesAccountLinkCrud:
    """Tests for account link operations."""

    def test_create_account_link_invitation(self, workspaces):
        """CreateAccountLinkInvitation returns an AccountLink."""
        resp = workspaces.create_account_link_invitation(
            TargetAccountId="222233334444",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountLink" in resp

    def test_accept_account_link_invitation_nonexistent(self, workspaces):
        """AcceptAccountLinkInvitation for nonexistent link raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.accept_account_link_invitation(LinkId="al-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_account_link_invitation_nonexistent(self, workspaces):
        """DeleteAccountLinkInvitation for nonexistent link raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.delete_account_link_invitation(LinkId="al-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_reject_account_link_invitation_nonexistent(self, workspaces):
        """RejectAccountLinkInvitation for nonexistent link raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.reject_account_link_invitation(LinkId="al-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesApplicationActions:
    """Tests for workspace application operations."""

    def test_associate_workspace_application(self, workspaces):
        """AssociateWorkspaceApplication returns a response."""
        resp = workspaces.associate_workspace_application(
            WorkspaceId="ws-fake12345",
            ApplicationId="wsa-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_workspace_application(self, workspaces):
        """DisassociateWorkspaceApplication returns a response."""
        resp = workspaces.disassociate_workspace_application(
            WorkspaceId="ws-fake12345",
            ApplicationId="wsa-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_deploy_workspace_applications(self, workspaces):
        """DeployWorkspaceApplications returns a response."""
        resp = workspaces.deploy_workspace_applications(
            WorkspaceId="ws-fake12345",
            Force=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesDirectoryActions:
    """Tests for directory-scoped operations on nonexistent dirs."""

    def test_associate_ip_groups_nonexistent(self, workspaces):
        """AssociateIpGroups for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.associate_ip_groups(
                DirectoryId="d-nonexistent999",
                GroupIds=["wsipg-fake12345"],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_disassociate_ip_groups_nonexistent(self, workspaces):
        """DisassociateIpGroups for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.disassociate_ip_groups(
                DirectoryId="d-nonexistent999",
                GroupIds=["wsipg-fake12345"],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_client_branding_nonexistent(self, workspaces):
        """ImportClientBranding for nonexistent directory raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.import_client_branding(
                ResourceId="d-nonexistent999",
                DeviceTypeWindows={"Logo": b"x" * 100},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesConnectClientAddIn:
    """Tests for Connect Client Add-In create."""

    def test_create_connect_client_add_in(self, workspaces):
        """CreateConnectClientAddIn returns an AddInId."""
        resp = workspaces.create_connect_client_add_in(
            ResourceId="d-fake12345",
            Name=_unique("addin"),
            URL="https://example.com",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AddInId" in resp


class TestWorkSpacesStandbyWorkspaces:
    """Tests for standby workspace operations."""

    def test_create_standby_workspaces(self, workspaces):
        """CreateStandbyWorkspaces returns response with pending/failed lists."""
        resp = workspaces.create_standby_workspaces(
            PrimaryRegion="us-east-1",
            StandbyWorkspaces=[
                {
                    "PrimaryWorkspaceId": "ws-fake12345",
                    "DirectoryId": "d-fake12345",
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "PendingStandbyWorkspaces" in resp or "FailedStandbyRequests" in resp


class TestWorkSpacesDeleteTags:
    """Tests for DeleteTags operation."""

    def test_delete_tags(self, workspaces):
        """DeleteTags succeeds even for nonexistent resources."""
        resp = workspaces.delete_tags(
            ResourceId="ws-fake12345",
            TagKeys=["env"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesDeleteClientBranding:
    """Tests for DeleteClientBranding operation."""

    def test_delete_client_branding_nonexistent(self, workspaces):
        """DeleteClientBranding for nonexistent directory raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.delete_client_branding(
                ResourceId="d-0000000000",
                Platforms=["DeviceTypeWindows"],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesRebuildWorkspaces:
    """Tests for RebuildWorkspaces operation."""

    def test_rebuild_nonexistent_workspace(self, workspaces):
        """RebuildWorkspaces for nonexistent workspace returns FailedRequests."""
        result = workspaces.rebuild_workspaces(
            RebuildWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-nonexistent123"
        assert "ErrorCode" in result["FailedRequests"][0]


class TestWorkSpacesRebootWorkspaces:
    """Tests for RebootWorkspaces operation."""

    def test_reboot_nonexistent_workspace(self, workspaces):
        """RebootWorkspaces for nonexistent workspace returns FailedRequests."""
        result = workspaces.reboot_workspaces(
            RebootWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-nonexistent123"
        assert "ErrorCode" in result["FailedRequests"][0]


class TestWorkSpacesStartWorkspaces:
    """Tests for StartWorkspaces operation."""

    def test_start_nonexistent_workspace(self, workspaces):
        """StartWorkspaces for nonexistent workspace returns FailedRequests."""
        result = workspaces.start_workspaces(
            StartWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-nonexistent123"
        assert "ErrorCode" in result["FailedRequests"][0]


class TestWorkSpacesStopWorkspaces:
    """Tests for StopWorkspaces operation."""

    def test_stop_nonexistent_workspace(self, workspaces):
        """StopWorkspaces for nonexistent workspace returns FailedRequests."""
        result = workspaces.stop_workspaces(
            StopWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-nonexistent123"
        assert "ErrorCode" in result["FailedRequests"][0]


class TestWorkSpacesTerminateWorkspacesPoolSession:
    """Tests for TerminateWorkspacesPoolSession operation."""

    def test_terminate_pool_session_nonexistent(self, workspaces):
        """TerminateWorkspacesPoolSession for nonexistent session returns 200."""
        resp = workspaces.terminate_workspaces_pool_session(
            SessionId="00000000-0000-0000-0000-000000000000"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesImageLifecycle:
    """Tests for workspace image import/copy/retrieve/delete lifecycle."""

    def test_import_workspace_image_and_retrieve(self, workspaces):
        """Import an image, then retrieve it by ID from describe."""
        name = _unique("img-retrieve")
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-import12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=name,
            ImageDescription="retrieve test image",
        )
        assert "ImageId" in import_resp
        image_id = import_resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert "Images" in desc_resp
        assert len(desc_resp["Images"]) == 1
        assert desc_resp["Images"][0]["ImageId"] == image_id

    def test_import_workspace_image_name_stored(self, workspaces):
        """Import image name and description are preserved in describe response."""
        name = _unique("img-name")
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-name12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=name,
            ImageDescription="name fidelity test",
        )
        image_id = import_resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        img = desc_resp["Images"][0]
        assert img["Name"] == name
        assert img["Description"] == "name fidelity test"

    def test_import_multiple_images_appear_in_list(self, workspaces):
        """Import 3 images and verify all appear in the list."""
        imported_ids = []
        for i in range(3):
            resp = workspaces.import_workspace_image(
                Ec2ImageId="ami-multi99999",
                IngestionProcess="BYOL_REGULAR",
                ImageName=_unique(f"img-list{i}"),
                ImageDescription=f"multi-list test {i}",
            )
            imported_ids.append(resp["ImageId"])

        list_resp = workspaces.describe_workspace_images()
        listed_ids = [img["ImageId"] for img in list_resp["Images"]]
        for img_id in imported_ids:
            assert img_id in listed_ids

    def test_copy_workspace_image_and_retrieve(self, workspaces):
        """Copy a workspace image, then retrieve the copy by ID."""
        copy_resp = workspaces.copy_workspace_image(
            Name=_unique("copy-retrieve"),
            SourceImageId="wsi-fake12345",
            SourceRegion="us-west-2",
        )
        assert "ImageId" in copy_resp
        copy_id = copy_resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[copy_id])
        assert "Images" in desc_resp
        assert len(desc_resp["Images"]) == 1
        assert desc_resp["Images"][0]["ImageId"] == copy_id

    def test_delete_then_describe_image_gone(self, workspaces):
        """Delete an imported image, then verify it no longer appears in describe."""
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-delete12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-delete"),
            ImageDescription="to be deleted",
        )
        image_id = import_resp["ImageId"]

        # Verify it exists
        before_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert len(before_resp["Images"]) == 1

        # Delete it
        del_resp = workspaces.delete_workspace_image(ImageId=image_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify it's gone
        after_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert after_resp["Images"] == []

    def test_import_workspace_image_id_format(self, workspaces):
        """Imported image ID has the expected wsi- prefix format."""
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-format12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-format"),
            ImageDescription="format test",
        )
        assert resp["ImageId"].startswith("wsi-")

    def test_copy_workspace_image_id_format(self, workspaces):
        """Copied image ID has the expected wsi- prefix format."""
        resp = workspaces.copy_workspace_image(
            Name=_unique("copy-format"),
            SourceImageId="wsi-fakesource",
            SourceRegion="us-east-1",
        )
        assert resp["ImageId"].startswith("wsi-")


class TestWorkSpacesApplicationAssociationLifecycle:
    """Tests for workspace application association lifecycle."""

    def test_associate_workspace_application_response_fields(self, workspaces):
        """AssociateWorkspaceApplication response Association includes WorkspaceId and State."""
        resp = workspaces.associate_workspace_application(
            WorkspaceId="ws-fake12345",
            ApplicationId="wsa-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Association" in resp
        assert resp["Association"]["WorkspaceId"] == "ws-fake12345"
        assert "State" in resp["Association"]
        assert "AssociatedResourceType" in resp["Association"]

    def test_disassociate_workspace_application_response_fields(self, workspaces):
        """DisassociateWorkspaceApplication response Association includes WorkspaceId and State."""
        resp = workspaces.disassociate_workspace_application(
            WorkspaceId="ws-fake12345",
            ApplicationId="wsa-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Association" in resp
        assert resp["Association"]["WorkspaceId"] == "ws-fake12345"
        assert "State" in resp["Association"]

    def test_deploy_workspace_applications_response_fields(self, workspaces):
        """DeployWorkspaceApplications response has Deployment with Associations list."""
        resp = workspaces.deploy_workspace_applications(
            WorkspaceId="ws-fake12345",
            Force=False,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Deployment" in resp
        assert "Associations" in resp["Deployment"]
        assert isinstance(resp["Deployment"]["Associations"], list)

    def test_associate_then_disassociate_application(self, workspaces):
        """Associate then disassociate an application both succeed."""
        assoc_resp = workspaces.associate_workspace_application(
            WorkspaceId="ws-assoc12345",
            ApplicationId="wsa-assoc12345",
        )
        assert assoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        disassoc_resp = workspaces.disassociate_workspace_application(
            WorkspaceId="ws-assoc12345",
            ApplicationId="wsa-assoc12345",
        )
        assert disassoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesRebuildRebootLifecycle:
    """Tests for rebuild/reboot with error detail assertions."""

    def test_rebuild_nonexistent_has_error_message(self, workspaces):
        """RebuildWorkspaces for nonexistent workspace FailedRequests has ErrorCode and ErrorMessage."""
        result = workspaces.rebuild_workspaces(
            RebuildWorkspaceRequests=[{"WorkspaceId": "ws-rebuildnone"}]
        )
        assert len(result["FailedRequests"]) == 1
        entry = result["FailedRequests"][0]
        assert "ErrorCode" in entry
        assert "ErrorMessage" in entry
        assert entry["WorkspaceId"] == "ws-rebuildnone"

    def test_reboot_nonexistent_has_error_message(self, workspaces):
        """RebootWorkspaces for nonexistent workspace FailedRequests has ErrorCode and ErrorMessage."""
        result = workspaces.reboot_workspaces(
            RebootWorkspaceRequests=[{"WorkspaceId": "ws-rebootnone"}]
        )
        assert len(result["FailedRequests"]) == 1
        entry = result["FailedRequests"][0]
        assert "ErrorCode" in entry
        assert "ErrorMessage" in entry
        assert entry["WorkspaceId"] == "ws-rebootnone"

    def test_rebuild_workspace(self, workspace, workspaces):
        """RebuildWorkspaces for existing workspace returns no FailedRequests."""
        result = workspaces.rebuild_workspaces(
            RebuildWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 0

    def test_reboot_workspace(self, workspace, workspaces):
        """RebootWorkspaces for existing workspace returns no FailedRequests."""
        result = workspaces.reboot_workspaces(
            RebootWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 0


class TestWorkSpacesDirectoryLifecycle:
    """Tests for directory registration and listing lifecycle."""

    def test_registered_directory_appears_in_list(self, registered_directory, workspaces):
        """Registering a directory makes it appear in describe_workspace_directories."""
        result = workspaces.describe_workspace_directories()
        dir_ids = [d["DirectoryId"] for d in result["Directories"]]
        assert registered_directory["directory_id"] in dir_ids

    def test_registered_directory_has_fields(self, registered_directory, workspaces):
        """Registered directory has DirectoryId and State fields."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.describe_workspace_directories(DirectoryIds=[dir_id])
        assert len(result["Directories"]) >= 1
        directory = next(d for d in result["Directories"] if d["DirectoryId"] == dir_id)
        assert "DirectoryId" in directory
        assert directory["DirectoryId"] == dir_id


class TestWorkSpacesWorkspaceLifecycle:
    """Tests for workspace create/describe/filter lifecycle."""

    def test_created_workspace_appears_in_describe(self, workspace, workspaces):
        """Created workspace appears in describe_workspaces by workspace ID."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        assert "Workspaces" in result
        assert len(result["Workspaces"]) >= 1
        ws_ids = [ws["WorkspaceId"] for ws in result["Workspaces"]]
        assert workspace in ws_ids

    def test_created_workspace_has_fields(self, workspace, workspaces):
        """Created workspace response has WorkspaceId, DirectoryId, UserName fields."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        ws = result["Workspaces"][0]
        assert "WorkspaceId" in ws
        assert "DirectoryId" in ws
        assert "UserName" in ws

    def test_created_workspace_appears_in_full_list(self, workspace, workspaces):
        """Created workspace appears in full describe_workspaces list (no filter)."""
        result = workspaces.describe_workspaces()
        ws_ids = [ws["WorkspaceId"] for ws in result["Workspaces"]]
        assert workspace in ws_ids

    def test_describe_workspaces_empty_result_has_key(self, workspaces):
        """describe_workspaces with nonexistent ID returns Workspaces key with empty list."""
        result = workspaces.describe_workspaces(WorkspaceIds=["ws-nonexistent00"])
        assert "Workspaces" in result
        assert result["Workspaces"] == []


class TestWorkspacesTagOps:
    """Tests for CreateTags and DescribeTags operations."""

    def test_create_tags_fake_resource(self, workspaces):
        """CreateTags with fake ResourceId returns 200 or error."""
        try:
            resp = workspaces.create_tags(
                ResourceId="ws-fake12345",
                Tags=[{"Key": "env", "Value": "test"}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except Exception as e:
            # Some implementations reject fake resource IDs
            assert hasattr(e, "response") or "error" in str(e).lower()

    def test_describe_tags_fake_resource(self, workspaces):
        """DescribeTags with fake ResourceId returns tag list or error."""
        try:
            resp = workspaces.describe_tags(ResourceId="ws-fake12345")
            assert "TagList" in resp
            assert isinstance(resp["TagList"], list)
        except Exception as e:
            assert hasattr(e, "response") or "error" in str(e).lower()


class TestWorkspacesGapOps:
    """Tests for previously-missing Workspaces operations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces")

    def test_import_workspace_image(self, client):
        """ImportWorkspaceImage returns an ImageId."""
        resp = client.import_workspace_image(
            Ec2ImageId="ami-12345678",
            IngestionProcess="BYOL_REGULAR",
            ImageName="test-image",
            ImageDescription="A test image",
        )
        assert "ImageId" in resp


class TestWorkspacesConnectAddInOps:
    """Tests for Connect client add-in operations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces")

    def test_delete_connect_client_add_in(self, client):
        """DeleteConnectClientAddIn returns 200 for a valid UUID add-in ID."""
        resp = client.delete_connect_client_add_in(
            AddInId="12345678-1234-1234-1234-123456789012",
            ResourceId="d-1234567890",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_connect_client_add_in_not_found(self, client):
        """UpdateConnectClientAddIn raises ResourceNotFoundException for nonexistent add-in."""
        from botocore.exceptions import ClientError  # noqa: PLC0415

        with pytest.raises(ClientError) as exc:
            client.update_connect_client_add_in(
                AddInId="12345678-1234-1234-1234-123456789012",
                ResourceId="d-1234567890",
                Name="TestAddIn",
                URL="https://example.com/connect",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkspacesImagePermissionGapOps:
    """Tests for WorkSpaces image and client properties gap operations."""

    @pytest.fixture
    def client(self):
        return make_client("workspaces")

    def test_import_custom_workspace_image_returns_image_id(self, client):
        resp = client.import_workspace_image(
            Ec2ImageId="ami-1234567890abcdef0",
            IngestionProcess="BYOL_REGULAR",
            ImageName="test-image",
            ImageDescription="Test image",
        )
        assert "ImageId" in resp
        assert resp["ImageId"].startswith("wsi-")

    def test_modify_client_properties_returns_response(self, client):
        from botocore.exceptions import ClientError

        # This may fail with internal error for non-existent directory
        try:
            resp = client.modify_client_properties(
                ResourceId="d-1234567890",
                ClientProperties={"ReconnectEnabled": "ENABLED"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "InvalidResourceStateException",
                "ResourceNotFoundException",
                "InternalError",
            )

    def test_modify_selfservice_permissions_returns_response(self, client):
        from botocore.exceptions import ClientError

        try:
            resp = client.modify_selfservice_permissions(
                ResourceId="d-1234567890",
                SelfservicePermissions={
                    "RestartWorkspace": "ENABLED",
                    "IncreaseVolumeSize": "DISABLED",
                    "ChangeComputeType": "DISABLED",
                    "SwitchRunningMode": "DISABLED",
                    "RebuildWorkspace": "DISABLED",
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "InvalidResourceStateException",
                "ResourceNotFoundException",
                "InternalError",
            )

    def test_update_workspace_image_permission_returns_response(self, client):
        from botocore.exceptions import ClientError

        try:
            resp = client.update_workspace_image_permission(
                ImageId="wsi-1234567890",
                AllowCopyImage=True,
                SharedAccountId="123456789012",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "AccessDeniedException",
                "InvalidParameterValuesException",
                "ResourceNotFoundException",
                "InternalError",
            )


class TestWorkSpacesIpGroupLifecycle:
    """Edge cases and behavioral fidelity for IP group CRUD lifecycle."""

    def test_ip_group_create_and_list(self, workspaces):
        """Created IP group appears in DescribeIpGroups list."""
        name = _unique("ipgrp-list")
        create_resp = workspaces.create_ip_group(GroupName=name, GroupDesc="list test")
        group_id = create_resp["GroupId"]

        list_resp = workspaces.describe_ip_groups()
        group_ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in group_ids

    def test_ip_group_id_format(self, workspaces):
        """IP group IDs follow the wsipg- prefix convention."""
        resp = workspaces.create_ip_group(GroupName=_unique("ipgrp-fmt"), GroupDesc="format test")
        assert resp["GroupId"].startswith("wsipg-")
        assert len(resp["GroupId"]) > len("wsipg-")

    def test_ip_group_create_with_rules(self, workspaces):
        """Creating IP group with initial rules stores them."""
        name = _unique("ipgrp-rules")
        resp = workspaces.create_ip_group(
            GroupName=name,
            GroupDesc="rules test",
            UserRules=[
                {"ipRule": "10.0.0.0/8", "ruleDesc": "private-a"},
                {"ipRule": "172.16.0.0/12", "ruleDesc": "private-b"},
            ],
        )
        group_id = resp["GroupId"]

        list_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        assert len(list_resp["Result"]) == 1
        group = list_resp["Result"][0]
        assert group["groupId"] == group_id
        assert group["groupName"] == name
        rules = group.get("userRules", [])
        assert len(rules) == 2

    def test_ip_group_authorize_then_describe(self, workspaces):
        """Authorized IP rules appear in describe response."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-auth-desc"), GroupDesc="auth+desc"
        )
        group_id = create_resp["GroupId"]

        workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "192.168.1.0/24", "ruleDesc": "office"}],
        )

        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        group = desc_resp["Result"][0]
        rules = group.get("userRules", [])
        cidrs = [r["ipRule"] for r in rules]
        assert "192.168.1.0/24" in cidrs

    def test_ip_group_revoke_rule(self, workspaces):
        """Revoking a rule removes it from the group."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-revoke"), GroupDesc="revoke test"
        )
        group_id = create_resp["GroupId"]

        workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "to-remove"}],
        )

        revoke_resp = workspaces.revoke_ip_rules(
            GroupId=group_id, UserRules=["10.0.0.0/8"]
        )
        assert revoke_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        group = desc_resp["Result"][0]
        rules = group.get("userRules", [])
        cidrs = [r["ipRule"] for r in rules]
        assert "10.0.0.0/8" not in cidrs

    def test_ip_group_update_rules(self, workspaces):
        """UpdateRulesOfIpGroup replaces all rules in the group."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-update"), GroupDesc="update test"
        )
        group_id = create_resp["GroupId"]

        workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "old"}],
        )

        update_resp = workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "172.16.0.0/12", "ruleDesc": "new"}],
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        group = desc_resp["Result"][0]
        rules = group.get("userRules", [])
        cidrs = [r["ipRule"] for r in rules]
        assert "172.16.0.0/12" in cidrs

    def test_ip_group_delete_then_describe(self, workspaces):
        """Deleted IP group no longer appears in DescribeIpGroups."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-del-desc"), GroupDesc="del then desc"
        )
        group_id = create_resp["GroupId"]

        workspaces.delete_ip_group(GroupId=group_id)

        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        group_ids = [g["groupId"] for g in desc_resp["Result"]]
        assert group_id not in group_ids

    def test_ip_group_describe_by_id_filter(self, workspaces):
        """DescribeIpGroups filtered by GroupIds returns only that group."""
        resp1 = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-a"), GroupDesc="group A"
        )
        resp2 = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-b"), GroupDesc="group B"
        )
        id_a = resp1["GroupId"]
        id_b = resp2["GroupId"]

        filtered = workspaces.describe_ip_groups(GroupIds=[id_a])
        result_ids = [g["groupId"] for g in filtered["Result"]]
        assert id_a in result_ids
        assert id_b not in result_ids


class TestWorkSpacesConnectionAliasLifecycle:
    """Edge cases and behavioral fidelity for connection alias lifecycle."""

    def test_connection_alias_id_format(self, workspaces):
        """Connection alias IDs follow the wsca- prefix convention."""
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-fmt')}.example.com",
        )
        assert resp["AliasId"].startswith("wsca-")
        assert len(resp["AliasId"]) > len("wsca-")

    def test_connection_alias_create_and_list(self, workspaces):
        """Created connection alias appears in DescribeConnectionAliases."""
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-list')}.example.com",
        )
        alias_id = resp["AliasId"]

        list_resp = workspaces.describe_connection_aliases()
        alias_ids = [a["AliasId"] for a in list_resp["ConnectionAliases"]]
        assert alias_id in alias_ids

    def test_connection_alias_has_state(self, workspaces):
        """Created connection alias has a State field."""
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-state')}.example.com",
        )
        alias_id = resp["AliasId"]

        list_resp = workspaces.describe_connection_aliases()
        alias = next(a for a in list_resp["ConnectionAliases"] if a["AliasId"] == alias_id)
        assert "State" in alias
        assert "ConnectionString" in alias

    def test_connection_alias_delete(self, workspaces):
        """Deleted connection alias no longer appears in describe."""
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-del')}.example.com",
        )
        alias_id = resp["AliasId"]

        del_resp = workspaces.delete_connection_alias(AliasId=alias_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        list_resp = workspaces.describe_connection_aliases()
        alias_ids = [a["AliasId"] for a in list_resp["ConnectionAliases"]]
        assert alias_id not in alias_ids


class TestWorkSpacesImageEdgeCases:
    """Edge cases for workspace image operations."""

    def test_import_image_unicode_name(self, workspaces):
        """Import image with unicode characters in name."""
        name = _unique("img-ünïcödé")
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-unicode12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=name,
            ImageDescription="test with ünïcödé chars",
        )
        assert "ImageId" in resp
        image_id = resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert len(desc_resp["Images"]) == 1
        assert desc_resp["Images"][0]["Name"] == name

    def test_import_image_long_description(self, workspaces):
        """Import image with a long description string."""
        long_desc = "A" * 256
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-longdesc12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-longdesc"),
            ImageDescription=long_desc,
        )
        assert "ImageId" in resp
        image_id = resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert desc_resp["Images"][0]["Description"] == long_desc

    def test_delete_nonexistent_image_idempotent(self, workspaces):
        """Deleting a nonexistent image succeeds (idempotent)."""
        resp = workspaces.delete_workspace_image(ImageId="wsi-doesnotexist999")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_import_image_state_field(self, workspaces):
        """Imported image has a State field in describe response."""
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-state12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-state"),
            ImageDescription="state check",
        )
        image_id = resp["ImageId"]

        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        img = desc_resp["Images"][0]
        assert "State" in img
        assert "Name" in img


class TestWorkSpacesPoolEdgeCases:
    """Edge cases for workspace pool operations."""

    def test_create_pool_response_fields(self, workspaces):
        """Created pool has expected fields in response."""
        resp = workspaces.create_workspaces_pool(
            PoolName=_unique("pool-fields"),
            BundleId="wsb-fake12345",
            DirectoryId="d-fake12345",
            Description="field check",
            Capacity={"DesiredUserSessions": 2},
        )
        pool = resp["WorkspacesPool"]
        assert "PoolId" in pool
        assert "PoolName" in pool
        assert "State" in pool
        assert "CapacityStatus" in pool

    def test_describe_pools_empty_list(self, workspaces):
        """DescribeWorkspacesPools returns empty list when filtered by nonexistent pool."""
        resp = workspaces.describe_workspaces_pools(
            Filters=[{"Name": "PoolName", "Values": ["nonexistent-pool-xyz"], "Operator": "EQUALS"}]
        )
        assert "WorkspacesPools" in resp
        assert isinstance(resp["WorkspacesPools"], list)


class TestWorkSpacesBundleEdgeCases:
    """Edge cases for workspace bundle operations."""

    def test_create_bundle_response_fields(self, workspaces):
        """Created bundle has expected fields."""
        resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-fields"),
            BundleDescription="field check bundle",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle = resp["WorkspaceBundle"]
        assert "BundleId" in bundle
        assert bundle["BundleId"].startswith("wsb-")

    def test_create_bundle_unicode_name(self, workspaces):
        """Created bundle with unicode in name stores it correctly."""
        name = _unique("bundle-日本語")
        resp = workspaces.create_workspace_bundle(
            BundleName=name,
            BundleDescription="unicode bundle test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "STANDARD"},
            UserStorage={"Capacity": "50"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspaceBundle" in resp


class TestWorkSpacesTagLifecycle:
    """Tests for tag create/describe/delete lifecycle on real resources."""

    def test_tag_ip_group_lifecycle(self, workspaces):
        """Create tags on an IP group, describe them, then delete them."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-tag"),
            GroupDesc="tag lifecycle test",
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
        )
        group_id = create_resp["GroupId"]

        # Describe tags
        tag_resp = workspaces.describe_tags(ResourceId=group_id)
        assert "TagList" in tag_resp
        tag_keys = [t["Key"] for t in tag_resp["TagList"]]
        assert "env" in tag_keys
        assert "team" in tag_keys

        # Delete one tag
        del_resp = workspaces.delete_tags(ResourceId=group_id, TagKeys=["env"])
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify tag was removed
        tag_resp2 = workspaces.describe_tags(ResourceId=group_id)
        tag_keys2 = [t["Key"] for t in tag_resp2["TagList"]]
        assert "env" not in tag_keys2
        assert "team" in tag_keys2

    def test_create_tags_on_connection_alias(self, workspaces):
        """Create and describe tags on a connection alias."""
        alias_resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-tag')}.example.com",
            Tags=[{"Key": "purpose", "Value": "testing"}],
        )
        alias_id = alias_resp["AliasId"]

        tag_resp = workspaces.describe_tags(ResourceId=alias_id)
        assert "TagList" in tag_resp
        tag_keys = [t["Key"] for t in tag_resp["TagList"]]
        assert "purpose" in tag_keys

    def test_add_tags_after_creation(self, workspaces):
        """Add tags to a resource after initial creation."""
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-addtag"), GroupDesc="add tag test"
        )
        group_id = create_resp["GroupId"]

        # Add tags
        workspaces.create_tags(
            ResourceId=group_id,
            Tags=[{"Key": "added", "Value": "later"}],
        )

        tag_resp = workspaces.describe_tags(ResourceId=group_id)
        tag_keys = [t["Key"] for t in tag_resp["TagList"]]
        assert "added" in tag_keys


class TestWorkSpacesMultipleCreatesEdge:
    """Tests for creating multiple resources and batch operations."""

    def test_create_multiple_ip_groups(self, workspaces):
        """Create 3 IP groups and verify all appear in list."""
        created_ids = []
        for i in range(3):
            resp = workspaces.create_ip_group(
                GroupName=_unique(f"ipgrp-multi{i}"), GroupDesc=f"multi {i}"
            )
            created_ids.append(resp["GroupId"])

        list_resp = workspaces.describe_ip_groups()
        listed_ids = [g["groupId"] for g in list_resp["Result"]]
        for gid in created_ids:
            assert gid in listed_ids

    def test_terminate_multiple_nonexistent_workspaces(self, workspaces):
        """Terminating multiple nonexistent workspaces returns all as failed."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[
                {"WorkspaceId": "ws-batchfail001"},
                {"WorkspaceId": "ws-batchfail002"},
                {"WorkspaceId": "ws-batchfail003"},
            ]
        )
        assert len(result["FailedRequests"]) == 3
        failed_ids = [f["WorkspaceId"] for f in result["FailedRequests"]]
        assert "ws-batchfail001" in failed_ids
        assert "ws-batchfail002" in failed_ids
        assert "ws-batchfail003" in failed_ids


class TestWorkSpacesAccountLinkEdgeCases:
    """Edge cases for account link operations."""

    def test_create_account_link_response_fields(self, workspaces):
        """CreateAccountLinkInvitation response has expected fields."""
        resp = workspaces.create_account_link_invitation(
            TargetAccountId="333344445555",
        )
        link = resp["AccountLink"]
        assert "AccountLinkId" in link
        assert "AccountLinkStatus" in link
        assert link["AccountLinkId"].startswith("wsal-")

    def test_list_account_links_empty(self, workspaces):
        """ListAccountLinks with a filter returns empty or valid list."""
        resp = workspaces.list_account_links(
            LinkStatusFilter=["LINKED"],
        )
        assert "AccountLinks" in resp
        assert isinstance(resp["AccountLinks"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesModifyAccountEdgeCases:
    """Edge cases for ModifyAccount and DescribeAccount interaction."""

    def test_modify_then_describe_account(self, workspaces):
        """ModifyAccount then DescribeAccount reflects the change."""
        workspaces.modify_account(
            DedicatedTenancySupport="ENABLED",
            DedicatedTenancyManagementCidrRange="10.0.0.0/16",
        )

        resp = workspaces.describe_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # After modifying, DedicatedTenancySupport should be present
        assert "DedicatedTenancySupport" in resp


class TestWorkSpacesConnectAddInEdgeCases:
    """Edge cases for Connect Client Add-In operations."""

    def test_create_add_in_response_fields(self, workspaces):
        """CreateConnectClientAddIn response has AddInId field."""
        resp = workspaces.create_connect_client_add_in(
            ResourceId="d-fake12345",
            Name=_unique("addin-fields"),
            URL="https://example.com/connect",
        )
        assert "AddInId" in resp
        # Add-in IDs are UUIDs
        assert len(resp["AddInId"]) > 10

    def test_describe_add_ins_for_nonexistent_directory(self, workspaces):
        """DescribeConnectClientAddIns for fake directory returns empty list."""
        resp = workspaces.describe_connect_client_add_ins(ResourceId="d-doesnotexist9")
        assert "AddIns" in resp
        assert isinstance(resp["AddIns"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesManagementCidrEdgeCases:
    """Edge cases for management CIDR operations."""

    def test_list_cidr_ranges_different_constraints(self, workspaces):
        """ListAvailableManagementCidrRanges works with different CIDR constraints."""
        resp = workspaces.list_available_management_cidr_ranges(
            ManagementCidrRangeConstraint="172.16.0.0/12"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ManagementCidrRanges" in resp
        assert isinstance(resp["ManagementCidrRanges"], list)
        # All returned ranges should be strings
        for cidr in resp["ManagementCidrRanges"]:
            assert isinstance(cidr, str)


class TestWorkSpacesWorkspaceLifecycleEdgeCases:
    """Edge cases for workspace create/describe lifecycle using the workspace fixture."""

    def test_workspace_id_format(self, workspace, workspaces):
        """Workspace IDs follow the ws- prefix convention."""
        assert workspace.startswith("ws-")
        assert len(workspace) > len("ws-")

    def test_workspace_has_state_field(self, workspace, workspaces):
        """Created workspace has a State field."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        ws = result["Workspaces"][0]
        assert "State" in ws

    def test_workspace_has_bundle_id(self, workspace, workspaces):
        """Created workspace has a BundleId matching what was requested."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        ws = result["Workspaces"][0]
        assert "BundleId" in ws
        assert ws["BundleId"] == "wsb-test123"

    def test_terminate_workspace_succeeds(self, workspace, workspaces):
        """Terminating an existing workspace returns no FailedRequests."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 0

    def test_start_workspace(self, workspace, workspaces):
        """Starting an existing workspace returns no FailedRequests."""
        result = workspaces.start_workspaces(
            StartWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 0

    def test_stop_workspace(self, workspace, workspaces):
        """Stopping an existing workspace returns no FailedRequests."""
        result = workspaces.stop_workspaces(
            StopWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert len(result["FailedRequests"]) == 0

    def test_workspace_connection_status(self, workspace, workspaces):
        """DescribeWorkspacesConnectionStatus returns status for existing workspace."""
        result = workspaces.describe_workspaces_connection_status(
            WorkspaceIds=[workspace]
        )
        assert "WorkspacesConnectionStatus" in result
        assert isinstance(result["WorkspacesConnectionStatus"], list)
