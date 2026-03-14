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
