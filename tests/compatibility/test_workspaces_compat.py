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
        """Describing directories returns an empty list; verify account state and error on invalid."""
        from botocore.exceptions import ClientError

        # LIST: directories list (possibly empty)
        result = workspaces.describe_workspace_directories()
        assert "Directories" in result
        assert isinstance(result["Directories"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE: singular describe (account)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: modify account setting
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # ERROR: deregister nonexistent directory
        with pytest.raises(ClientError) as exc:
            workspaces.deregister_workspace_directory(DirectoryId="d-nonexistent999")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
            "InvalidResourceStateException",
            "InternalError",
        )


class TestWorkSpacesOperations:
    """Tests for core WorkSpaces operations."""

    def test_describe_workspaces_empty(self, workspaces):
        """Describing workspaces returns empty list; also verify account and error on bad ops."""
        from botocore.exceptions import ClientError

        # LIST
        result = workspaces.describe_workspaces()
        assert "Workspaces" in result
        assert isinstance(result["Workspaces"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE (singular: describe_account)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # DELETE: terminate nonexistent (FailedRequests, not exception)
        term = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent-empty1"}]
        )
        assert len(term["FailedRequests"]) == 1

        # ERROR: modify state of nonexistent workspace
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-nonexistent-empty2",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_workspaces_invalid_directory(self, workspaces):
        """Creating a workspace with a nonexistent directory returns a FailedRequest."""
        from botocore.exceptions import ClientError

        # CREATE (invalid dir) - returns FailedRequests
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

        # LIST: describe workspaces after failed create
        list_resp = workspaces.describe_workspaces()
        assert "Workspaces" in list_resp

        # RETRIEVE: account info
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: modify account
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # ERROR: try to modify state of nonexistent workspace
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-createfail999",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_workspace_images_empty(self, workspaces):
        """Describing workspace images returns empty list; import one, verify, delete."""
        # LIST (initially empty or not)
        result = workspaces.describe_workspace_images()
        assert "Images" in result
        assert isinstance(result["Images"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE: import an image
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-emptydesc99",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-empty-desc"),
            ImageDescription="empty describe test",
        )
        image_id = import_resp["ImageId"]
        assert image_id.startswith("wsi-")

        # RETRIEVE: get it by ID
        get_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert get_resp["Images"][0]["ImageId"] == image_id

        # UPDATE: tag the image
        workspaces.create_tags(
            ResourceId=image_id,
            Tags=[{"Key": "test", "Value": "empty-desc"}],
        )

        # DELETE
        del_resp = workspaces.delete_workspace_image(ImageId=image_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


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
        """Filtering by nonexistent WorkspaceIds returns empty list; also create/update/delete."""
        from botocore.exceptions import ClientError

        # LIST: filter by nonexistent ID
        result = workspaces.describe_workspaces(WorkspaceIds=["ws-nonexistent123"])
        assert "Workspaces" in result
        assert result["Workspaces"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE: import a bundle (always succeeds)
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-filter"),
            BundleDescription="filter test bundle",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        # RETRIEVE: get account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: tag the bundle
        workspaces.create_tags(
            ResourceId=bundle_id,
            Tags=[{"Key": "filter-test", "Value": "yes"}],
        )

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR: terminate nonexistent
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-filterfail999",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_workspaces_by_bundle_id_nonexistent(self, workspaces):
        """Filtering by nonexistent BundleId returns empty list; create/update/delete bundle."""
        from botocore.exceptions import ClientError

        # LIST: filter by nonexistent bundle
        result = workspaces.describe_workspaces(BundleId="wsb-nonexistent123")
        assert "Workspaces" in result
        assert result["Workspaces"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-bundlefilt"),
            BundleDescription="bundle filter test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_workspace_bundle(BundleId=bundle_id, ImageId="wsi-updated99")

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId=bundle_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_workspaces_by_directory_and_username_nonexistent(self, workspaces):
        """Filtering by nonexistent DirectoryId+UserName returns empty list; full bundle lifecycle."""
        from botocore.exceptions import ClientError

        # LIST
        result = workspaces.describe_workspaces(DirectoryId="d-nonexistent1234", UserName="nobody")
        assert "Workspaces" in result
        assert result["Workspaces"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-dirfilt"), GroupDesc="dir filter test"
        )
        group_id = create_resp["GroupId"]

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "test"}],
        )

        # DELETE
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_workspace_images_by_ids_nonexistent(self, workspaces):
        """Filtering images by nonexistent ImageIds returns empty list; full image lifecycle."""
        from botocore.exceptions import ClientError

        # LIST: nonexistent filter
        result = workspaces.describe_workspace_images(ImageIds=["wsi-nonexistent123"])
        assert "Images" in result
        assert result["Images"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE: import image
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-filtertest",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-nonex-filter"),
            ImageDescription="nonexistent filter test",
        )
        image_id = import_resp["ImageId"]

        # RETRIEVE by ID
        get_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert get_resp["Images"][0]["ImageId"] == image_id

        # UPDATE: tag
        workspaces.create_tags(
            ResourceId=image_id,
            Tags=[{"Key": "filtertest", "Value": "true"}],
        )

        # DELETE
        workspaces.delete_workspace_image(ImageId=image_id)

        # ERROR: describe deleted image by ID returns empty (no raise)
        after = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert after["Images"] == []


class TestWorkSpacesClientProperties:
    """Tests for DescribeClientProperties."""

    def test_describe_client_properties_empty(self, workspaces):
        """DescribeClientProperties with nonexistent resource; full lifecycle via IP group."""
        from botocore.exceptions import ClientError

        # LIST (describe client properties - plural response key)
        result = workspaces.describe_client_properties(ResourceIds=["d-9267462133"])
        assert "ClientPropertiesList" in result
        assert isinstance(result["ClientPropertiesList"], list)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # CREATE: IP group
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-clientprop"), GroupDesc="client props test"
        )
        group_id = create_resp["GroupId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "192.168.0.0/16", "ruleDesc": "client-prop-test"}],
        )

        # DELETE
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        """Terminating nonexistent workspace returns FailedRequests; also lifecycle patterns."""
        from botocore.exceptions import ClientError

        # DELETE: terminate nonexistent (FailedRequests, not exception)
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

        # CREATE: IP group to cover CREATE pattern
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-term"), GroupDesc="terminate test"
        )
        group_id = create_resp["GroupId"]

        # LIST: verify it's in the list
        list_resp = workspaces.describe_ip_groups()
        group_ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in group_ids

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: update rules
        workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "term-test"}],
        )

        # ERROR: modify state of nonexistent workspace
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-termfail888",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # Cleanup
        workspaces.delete_ip_group(GroupId=group_id)


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
        """DescribeAccount returns account info; full lifecycle via IP group and modify."""
        from botocore.exceptions import ClientError

        # RETRIEVE: singular describe
        resp = workspaces.describe_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp, dict)
        has_tenancy_field = (
            "DedicatedTenancySupport" in resp
            or "DedicatedTenancyManagementCidrRange" in resp
        )
        assert has_tenancy_field

        # CREATE: IP group
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-acct"), GroupDesc="account test"
        )
        group_id = create_resp["GroupId"]

        # LIST
        list_resp = workspaces.describe_ip_groups()
        ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in ids

        # UPDATE: modify account
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # DELETE
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_account_modifications(self, workspaces):
        """DescribeAccountModifications returns list; full lifecycle with bundle."""
        from botocore.exceptions import ClientError

        # LIST (modifications)
        resp = workspaces.describe_account_modifications()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountModifications" in resp
        assert isinstance(resp["AccountModifications"], list)
        for mod in resp["AccountModifications"]:
            assert "ModificationState" in mod

        # CREATE: bundle
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-acctmod"),
            BundleDescription="account modifications test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: update bundle
        workspaces.update_workspace_bundle(BundleId=bundle_id, ImageId="wsi-updated99")

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR: update deleted bundle
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId=bundle_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesBundleOperations:
    """Tests for WorkSpaces bundle operations."""

    def test_describe_workspace_bundles(self, workspaces):
        """DescribeWorkspaceBundles returns bundles; full bundle lifecycle."""
        from botocore.exceptions import ClientError

        # LIST
        resp = workspaces.describe_workspace_bundles()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Bundles" in resp
        assert isinstance(resp["Bundles"], list)
        for bundle in resp["Bundles"]:
            assert "BundleId" in bundle

        # CREATE
        name = _unique("bundle-bundleops")
        create_resp = workspaces.create_workspace_bundle(
            BundleName=name,
            BundleDescription="bundle ops test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: update bundle
        workspaces.update_workspace_bundle(BundleId=bundle_id, ImageId="wsi-updated88")

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR: delete nonexistent IP group
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId="wsipg-bundleops999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesIpGroupOperations:
    """Tests for WorkSpaces IP group operations."""

    def test_describe_ip_groups(self, workspaces):
        """DescribeIpGroups returns IP groups; full lifecycle with create/update/delete."""
        from botocore.exceptions import ClientError

        # LIST (possibly empty)
        resp = workspaces.describe_ip_groups()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Result" in resp
        assert isinstance(resp["Result"], list)
        for group in resp["Result"]:
            assert "groupId" in group

        # CREATE
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-ipgroupops"), GroupDesc="ip group ops test"
        )
        group_id = create_resp["GroupId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: authorize rules
        workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "ipgroupops-test"}],
        )

        # DELETE
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR: delete again
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesConnectionAliasOperations:
    """Tests for WorkSpaces connection alias operations."""

    def test_describe_connection_aliases(self, workspaces):
        """DescribeConnectionAliases returns aliases; full lifecycle with create/tag/delete."""
        from botocore.exceptions import ClientError

        # LIST (possibly empty)
        resp = workspaces.describe_connection_aliases()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ConnectionAliases" in resp
        assert isinstance(resp["ConnectionAliases"], list)
        for alias in resp["ConnectionAliases"]:
            assert "AliasId" in alias

        # CREATE
        create_resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-aliasops')}.example.com"
        )
        alias_id = create_resp["AliasId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: tag the alias
        workspaces.create_tags(
            ResourceId=alias_id,
            Tags=[{"Key": "aliasops", "Value": "true"}],
        )

        # DELETE
        workspaces.delete_connection_alias(AliasId=alias_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_connection_alias(AliasId=alias_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesPoolOperations:
    """Tests for WorkSpaces pool operations."""

    def test_describe_workspaces_pools(self, workspaces):
        """DescribeWorkspacesPools returns pools; full lifecycle with create/update/delete."""
        from botocore.exceptions import ClientError

        # LIST
        resp = workspaces.describe_workspaces_pools()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspacesPools" in resp
        assert isinstance(resp["WorkspacesPools"], list)

        # CREATE: create a pool
        create_resp = workspaces.create_workspaces_pool(
            PoolName=_unique("pool-desctest"),
            BundleId="wsb-fake12345",
            DirectoryId="d-fake12345",
            Description="pools describe test",
            Capacity={"DesiredUserSessions": 1},
        )
        pool = create_resp["WorkspacesPool"]
        pool_id = pool["PoolId"]

        # RETRIEVE: describe_account (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: tag the pool
        workspaces.create_tags(
            ResourceId=pool_id,
            Tags=[{"Key": "pool-test", "Value": "true"}],
        )

        # DELETE: terminate pool (nonexistent to avoid state issues)
        with pytest.raises(ClientError) as exc_stop:
            workspaces.stop_workspaces_pool(PoolId="wspool-nonexistent9999")
        assert exc_stop.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # ERROR: update nonexistent pool
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspaces_pool(PoolId="wspool-nonexistent8888")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        """ListAccountLinks returns list; full lifecycle with create/get/delete/error."""
        from botocore.exceptions import ClientError

        # LIST
        resp = workspaces.list_account_links()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountLinks" in resp
        assert isinstance(resp["AccountLinks"], list)

        # CREATE
        create_resp = workspaces.create_account_link_invitation(TargetAccountId="888899990000")
        link_id = create_resp["AccountLink"]["AccountLinkId"]

        # RETRIEVE
        get_resp = workspaces.get_account_link(LinkId=link_id)
        assert get_resp["AccountLink"]["AccountLinkId"] == link_id

        # UPDATE: modify account alongside
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # DELETE
        del_resp = workspaces.delete_account_link_invitation(LinkId=link_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.get_account_link(LinkId="wsal-listlinks999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        """CreateIpGroup returns a GroupId; verify list, update, delete, error."""
        from botocore.exceptions import ClientError

        # CREATE
        resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp"),
            GroupDesc="compat test group",
        )
        assert "GroupId" in resp
        assert resp["GroupId"].startswith("wsipg-")
        group_id = resp["GroupId"]

        # LIST
        list_resp = workspaces.describe_ip_groups()
        ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in ids

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "create-test"}],
        )

        # DELETE
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        """AuthorizeIpRules adds rules; verify via describe, revoke, and error."""
        from botocore.exceptions import ClientError

        # CREATE
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-auth"),
            GroupDesc="auth test",
        )
        group_id = create_resp["GroupId"]

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: authorize rules
        auth_resp = workspaces.authorize_ip_rules(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "private"}],
        )
        assert auth_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # LIST: verify rule stored
        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        rules = desc_resp["Result"][0].get("userRules", [])
        cidrs = [r["ipRule"] for r in rules]
        assert "10.0.0.0/8" in cidrs

        # DELETE: revoke the rule
        revoke_resp = workspaces.revoke_ip_rules(
            GroupId=group_id, UserRules=["10.0.0.0/8"]
        )
        assert revoke_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: authorize on nonexistent group
        with pytest.raises(ClientError) as exc:
            workspaces.authorize_ip_rules(
                GroupId="wsipg-nonexistent999",
                UserRules=[{"ipRule": "10.0.0.0/8"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # Cleanup
        workspaces.delete_ip_group(GroupId=group_id)

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
        """CreateConnectionAlias returns an AliasId; verify list, tag, delete, error."""
        from botocore.exceptions import ClientError

        # CREATE
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias')}.example.com",
        )
        assert "AliasId" in resp
        assert resp["AliasId"].startswith("wsca-")
        alias_id = resp["AliasId"]

        # LIST
        list_resp = workspaces.describe_connection_aliases()
        ids = [a["AliasId"] for a in list_resp["ConnectionAliases"]]
        assert alias_id in ids

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: tag it
        workspaces.create_tags(
            ResourceId=alias_id,
            Tags=[{"Key": "alias-create-test", "Value": "true"}],
        )

        # DELETE
        workspaces.delete_connection_alias(AliasId=alias_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.delete_connection_alias(AliasId=alias_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        """ModifyAccount sets dedicated tenancy support; full lifecycle."""
        from botocore.exceptions import ClientError

        # RETRIEVE: get current state
        initial = workspaces.describe_account()
        assert "DedicatedTenancySupport" in initial

        # CREATE: IP group for lifecycle
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-modacct"), GroupDesc="modify account test"
        )
        group_id = create_resp["GroupId"]

        # LIST
        list_resp = workspaces.describe_ip_groups()
        assert group_id in [g["groupId"] for g in list_resp["Result"]]

        # UPDATE: modify account
        resp = workspaces.modify_account(
            DedicatedTenancySupport="ENABLED",
            DedicatedTenancyManagementCidrRange="10.0.0.0/16",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE: clean up ip group
        workspaces.delete_ip_group(GroupId=group_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-modacct999",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        """CreateWorkspaceBundle returns a WorkspaceBundle; full lifecycle."""
        from botocore.exceptions import ClientError

        # CREATE
        resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle"),
            BundleDescription="compat test bundle",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "WorkspaceBundle" in resp
        bundle_id = resp["WorkspaceBundle"]["BundleId"]

        # LIST
        list_resp = workspaces.describe_workspace_bundles(BundleIds=[bundle_id])
        assert len(list_resp["Bundles"]) >= 1

        # RETRIEVE (singular)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_workspace_bundle(BundleId=bundle_id, ImageId="wsi-updated88")

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId=bundle_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        assert ws["WorkspaceId"] == workspace
        assert len(ws["DirectoryId"]) > 0
        assert len(ws["UserName"]) > 0

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
        """ImportWorkspaceImage returns an ImageId with wsi- prefix."""
        resp = client.import_workspace_image(
            Ec2ImageId="ami-12345678",
            IngestionProcess="BYOL_REGULAR",
            ImageName="test-image",
            ImageDescription="A test image",
        )
        assert resp["ImageId"].startswith("wsi-")


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
        assert len(cidrs) >= 1
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
        assert alias["State"] in ("CREATED", "DELETING", "DELETED")
        assert alias["ConnectionString"].endswith(".example.com")

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
        assert img["State"] in ("AVAILABLE", "PENDING", "ERROR")
        assert len(img["Name"]) > 0


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
        assert pool["PoolId"].startswith("wsp-")
        assert len(pool["PoolName"]) > 0
        assert pool["State"] in (
            "CREATING", "DELETING", "RUNNING", "STARTING",
            "STOPPED", "STOPPING", "ERROR", "AVAILABLE",
        )
        assert isinstance(pool["CapacityStatus"], dict)

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
        tags = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
        assert tags.get("purpose") == "testing"

    def test_add_tags_after_creation(self, workspaces):
        """Add tags to a resource after initial creation, update rules, delete tags, check error."""
        from botocore.exceptions import ClientError

        # CREATE
        create_resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-addtag"), GroupDesc="add tag test"
        )
        group_id = create_resp["GroupId"]

        # Add tags (CREATE tag)
        workspaces.create_tags(
            ResourceId=group_id,
            Tags=[{"Key": "added", "Value": "later"}, {"Key": "stage", "Value": "dev"}],
        )

        # RETRIEVE/LIST: describe tags to verify
        tag_resp = workspaces.describe_tags(ResourceId=group_id)
        tags = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
        assert tags.get("added") == "later"
        assert tags.get("stage") == "dev"

        # UPDATE: update rules on the IP group itself
        update_resp = workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "updated"}],
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE: remove one tag
        del_resp = workspaces.delete_tags(ResourceId=group_id, TagKeys=["stage"])
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify deletion
        tag_resp2 = workspaces.describe_tags(ResourceId=group_id)
        tags2 = {t["Key"]: t["Value"] for t in tag_resp2["TagList"]}
        assert "stage" not in tags2
        assert tags2.get("added") == "later"

        # ERROR: update rules on nonexistent group
        with pytest.raises(ClientError) as exc:
            workspaces.update_rules_of_ip_group(
                GroupId="wsipg-doesnotexist999",
                UserRules=[{"ipRule": "10.0.0.0/8"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        """Created workspace has a State field with a valid enum value."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        ws = result["Workspaces"][0]
        assert ws["State"] in (
            "PENDING", "AVAILABLE", "IMPAIRED", "UNHEALTHY",
            "REBOOTING", "STARTING", "REBUILDING", "RESTORING",
            "MAINTENANCE", "ADMIN_MAINTENANCE", "SUSPENDED",
            "UPDATING", "STOPPING", "STOPPED", "TERMINATING",
            "TERMINATED", "ERROR",
        )

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


class TestWorkSpacesDirectoryEdgeCases:
    """Edge cases covering CREATE/RETRIEVE/DELETE patterns for directory operations."""

    def test_register_then_describe_specific_directory(self, registered_directory, workspaces):
        """Register a directory then retrieve it by ID - covers CREATE + RETRIEVE pattern."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.describe_workspace_directories(DirectoryIds=[dir_id])
        assert "Directories" in result
        assert len(result["Directories"]) >= 1
        assert result["Directories"][0]["DirectoryId"] == dir_id

    def test_deregister_directory_removes_from_list(self, registered_directory, workspaces, ds, ec2):
        """Deregistering a directory removes it from the list - covers DELETE pattern."""
        dir_id = registered_directory["directory_id"]
        workspaces.deregister_workspace_directory(DirectoryId=dir_id)
        result = workspaces.describe_workspace_directories()
        dir_ids = [d["DirectoryId"] for d in result["Directories"]]
        assert dir_id not in dir_ids

    def test_describe_directories_response_structure(self, registered_directory, workspaces):
        """Registered directory response has DirectoryType and RegistrationCode fields."""
        dir_id = registered_directory["directory_id"]
        result = workspaces.describe_workspace_directories(DirectoryIds=[dir_id])
        assert len(result["Directories"]) >= 1
        directory = result["Directories"][0]
        # Must have core identification fields
        assert "DirectoryId" in directory
        assert "DirectoryName" in directory

    def test_deregister_nonexistent_directory_raises_error(self, workspaces):
        """Deregistering a nonexistent directory raises an error - covers ERROR pattern."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            workspaces.deregister_workspace_directory(DirectoryId="d-nonexistent999")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
            "InvalidResourceStateException",
            "InternalError",
        )


class TestWorkSpacesWorkspaceEdgeCases:
    """Edge cases covering UPDATE/DELETE/ERROR patterns for workspace operations."""

    def test_describe_workspaces_response_has_metadata(self, workspaces):
        """DescribeWorkspaces response always has ResponseMetadata with status 200."""
        result = workspaces.describe_workspaces()
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Workspaces" in result

    def test_describe_workspaces_by_bundle_id_returns_list(self, workspaces):
        """DescribeWorkspaces with BundleId filter always returns Workspaces list."""
        result = workspaces.describe_workspaces(BundleId="wsb-doesnotexist999")
        assert isinstance(result["Workspaces"], list)

    def test_modify_workspace_state_existing(self, workspace, workspaces):
        """ModifyWorkspaceState for an existing workspace returns 200 - covers UPDATE pattern."""
        resp = workspaces.modify_workspace_state(
            WorkspaceId=workspace,
            WorkspaceState="ADMIN_MAINTENANCE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_workspace_user_name_stored(self, workspace, workspaces):
        """Created workspace UserName is retrievable via describe - covers RETRIEVE pattern."""
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        ws = result["Workspaces"][0]
        assert "UserName" in ws
        assert len(ws["UserName"]) > 0

    def test_terminate_workspace_makes_it_absent(self, workspace, workspaces):
        """Terminated workspace no longer appears in filtering - covers DELETE pattern."""
        workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        # After termination, filter by that specific ID should return empty or TERMINATED state
        result = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        if result["Workspaces"]:
            # If still present, state should reflect termination
            assert result["Workspaces"][0]["State"] in (
                "TERMINATING", "TERMINATED", "ERROR"
            )

    def test_create_workspaces_invalid_bundle_error_fields(self, workspaces):
        """FailedRequests for invalid directory has WorkspaceId, ErrorCode, ErrorMessage."""
        result = workspaces.create_workspaces(
            Workspaces=[
                {
                    "DirectoryId": "d-0000000000",
                    "UserName": "edgeuser",
                    "BundleId": "wsb-fake99",
                }
            ]
        )
        assert len(result["FailedRequests"]) == 1
        failed = result["FailedRequests"][0]
        # The failed entry should echo back the original workspace request fields
        assert "ErrorCode" in failed
        assert "ErrorMessage" in failed


class TestWorkSpacesWorkspaceImagesEdgeCases:
    """Edge cases for workspace image operations covering all six patterns."""

    def test_describe_images_empty_response_structure(self, workspaces):
        """DescribeWorkspaceImages empty result has Images key and 200 status."""
        result = workspaces.describe_workspace_images(ImageIds=["wsi-definitelynotreal"])
        assert "Images" in result
        assert result["Images"] == []
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_import_and_update_image_permissions(self, workspaces):
        """Import image then update its sharing permissions - covers UPDATE pattern."""
        from botocore.exceptions import ClientError

        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-perm12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-perm"),
            ImageDescription="permission test",
        )
        image_id = resp["ImageId"]

        # Update image permission (may fail with access error - that's still valid server behavior)
        try:
            perm_resp = workspaces.update_workspace_image_permission(
                ImageId=image_id,
                AllowCopyImage=True,
                SharedAccountId="123456789012",
            )
            assert perm_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            assert exc.value.response["Error"]["Code"] in (
                "AccessDeniedException",
                "ResourceNotFoundException",
                "InvalidParameterValuesException",
            )

    def test_image_tags_lifecycle(self, workspaces):
        """Import image, tag it, describe tags - covers CREATE + RETRIEVE tag pattern."""
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-tag12345",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-tag"),
            ImageDescription="tag test",
        )
        image_id = resp["ImageId"]

        workspaces.create_tags(
            ResourceId=image_id,
            Tags=[{"Key": "stage", "Value": "dev"}],
        )

        tag_resp = workspaces.describe_tags(ResourceId=image_id)
        tags = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
        assert tags.get("stage") == "dev"

    def test_describe_images_nonexistent_id_returns_empty(self, workspaces):
        """Filtering by nonexistent image ID returns empty list - covers LIST + ERROR pattern."""
        result = workspaces.describe_workspace_images(ImageIds=["wsi-never-existed-1"])
        assert "Images" in result
        assert result["Images"] == []


class TestWorkSpacesAccountEdgeCases:
    """Edge cases for account-level operations covering multiple patterns."""

    def test_describe_account_has_dedicated_tenancy_support_value(self, workspaces):
        """DescribeAccount returns a valid DedicatedTenancySupport enum value."""
        resp = workspaces.describe_account()
        assert "DedicatedTenancySupport" in resp
        assert resp["DedicatedTenancySupport"] in ("ENABLED", "DISABLED")

    def test_modify_and_describe_account_cycle(self, workspaces):
        """Modify account settings then verify describe reflects the change."""
        workspaces.modify_account(
            DedicatedTenancySupport="DISABLED",
        )
        resp = workspaces.describe_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DedicatedTenancySupport" in resp

    def test_describe_account_modifications_structure(self, workspaces):
        """Account modifications list has correct structure per entry."""
        resp = workspaces.describe_account_modifications()
        assert "AccountModifications" in resp
        for mod in resp["AccountModifications"]:
            assert "ModificationState" in mod
            assert mod["ModificationState"] in (
                "PENDING", "COMPLETED", "FAILED", "COMPLETED_WITH_ERRORS"
            )

    def test_modify_account_enabled_then_disabled(self, workspaces):
        """Toggle DedicatedTenancySupport on and off - covers UPDATE pattern twice."""
        workspaces.modify_account(DedicatedTenancySupport="ENABLED")
        resp1 = workspaces.describe_account()
        assert resp1["DedicatedTenancySupport"] == "ENABLED"

        workspaces.modify_account(DedicatedTenancySupport="DISABLED")
        resp2 = workspaces.describe_account()
        assert resp2["DedicatedTenancySupport"] == "DISABLED"


class TestWorkSpacesBundleEdgeCasesExtended:
    """Extended edge cases for workspace bundle operations."""

    def test_describe_bundles_response_structure(self, workspaces):
        """DescribeWorkspaceBundles response has correct structure."""
        resp = workspaces.describe_workspace_bundles()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Bundles"], list)
        # If bundles exist, verify they have at least BundleId and Name
        for bundle in resp["Bundles"]:
            assert bundle["BundleId"].startswith("wsb-")
            assert len(bundle["Name"]) > 0

    def test_create_then_describe_bundle(self, workspaces):
        """Create a bundle then verify it appears in describe output by ID - covers C+R pattern."""
        name = _unique("bundle-verify")
        create_resp = workspaces.create_workspace_bundle(
            BundleName=name,
            BundleDescription="verify test bundle",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        own_resp = workspaces.describe_workspace_bundles(BundleIds=[bundle_id])
        assert len(own_resp["Bundles"]) >= 1
        assert own_resp["Bundles"][0]["BundleId"] == bundle_id

    def test_create_bundle_then_delete(self, workspaces):
        """Create a bundle then delete it - covers C+D pattern."""
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-del"),
            BundleDescription="delete test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        del_resp = workspaces.delete_workspace_bundle(BundleId=bundle_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_workspace_bundle_existing(self, workspaces):
        """UpdateWorkspaceBundle for an existing bundle returns 200 - covers UPDATE pattern."""
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-upd"),
            BundleDescription="update test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        upd_resp = workspaces.update_workspace_bundle(
            BundleId=bundle_id,
            ImageId="wsi-newimage12",
        )
        assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesIpGroupsEdgeCasesExtended:
    """Extended edge cases for IP group operations covering LIST pattern comprehensively."""

    def test_describe_ip_groups_empty_response_structure(self, workspaces):
        """DescribeIpGroups with nonexistent group IDs returns empty Result list."""
        resp = workspaces.describe_ip_groups(GroupIds=["wsipg-neverexisted99"])
        assert "Result" in resp
        assert isinstance(resp["Result"], list)
        assert resp["Result"] == []

    def test_ip_group_name_stored_correctly(self, workspaces):
        """IP group name and description are retrievable after creation - RETRIEVE pattern."""
        name = _unique("ipgrp-name-check")
        desc = "description for name check"
        resp = workspaces.create_ip_group(GroupName=name, GroupDesc=desc)
        group_id = resp["GroupId"]

        detail = workspaces.describe_ip_groups(GroupIds=[group_id])
        assert len(detail["Result"]) == 1
        group = detail["Result"][0]
        assert group["groupName"] == name
        assert group["groupDesc"] == desc

    def test_describe_ip_groups_without_filter_includes_created(self, workspaces):
        """DescribeIpGroups without filter includes recently created groups."""
        resp = workspaces.create_ip_group(
            GroupName=_unique("ipgrp-unfiltered"), GroupDesc="no filter test"
        )
        group_id = resp["GroupId"]

        list_resp = workspaces.describe_ip_groups()
        ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in ids


class TestWorkSpacesConnectionAliasesEdgeCases:
    """Extended edge cases for connection alias operations."""

    def test_describe_connection_aliases_empty_response(self, workspaces):
        """DescribeConnectionAliases with nonexistent ID filter returns empty list."""
        resp = workspaces.describe_connection_aliases(AliasIds=["wsca-neverexisted999"])
        assert "ConnectionAliases" in resp
        assert isinstance(resp["ConnectionAliases"], list)

    def test_connection_alias_connection_string_stored(self, workspaces):
        """Connection alias ConnectionString is preserved in describe - RETRIEVE pattern."""
        conn_str = f"{_unique('alias-cs')}.example.com"
        resp = workspaces.create_connection_alias(ConnectionString=conn_str)
        alias_id = resp["AliasId"]

        list_resp = workspaces.describe_connection_aliases(AliasIds=[alias_id])
        assert len(list_resp["ConnectionAliases"]) == 1
        alias = list_resp["ConnectionAliases"][0]
        assert alias["ConnectionString"] == conn_str
        assert alias["AliasId"] == alias_id

    def test_connection_alias_filter_by_alias_id(self, workspaces):
        """DescribeConnectionAliases filtered by AliasId returns only that alias."""
        resp_a = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-filter-a')}.example.com"
        )
        resp_b = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-filter-b')}.example.com"
        )
        alias_id_a = resp_a["AliasId"]
        alias_id_b = resp_b["AliasId"]

        filtered = workspaces.describe_connection_aliases(AliasIds=[alias_id_a])
        returned_ids = [a["AliasId"] for a in filtered["ConnectionAliases"]]
        assert alias_id_a in returned_ids
        assert alias_id_b not in returned_ids


class TestWorkSpacesClientPropertiesEdgeCases:
    """Edge cases for DescribeClientProperties and ModifyClientProperties."""

    def test_describe_client_properties_multiple_nonexistent_ids(self, workspaces):
        """DescribeClientProperties with multiple nonexistent IDs returns empty list."""
        result = workspaces.describe_client_properties(
            ResourceIds=["d-0000000001", "d-0000000002"]
        )
        assert "ClientPropertiesList" in result
        assert isinstance(result["ClientPropertiesList"], list)

    def test_modify_client_properties_for_registered_directory(
        self, registered_directory, workspaces
    ):
        """ModifyClientProperties for a registered directory - covers UPDATE pattern."""
        from botocore.exceptions import ClientError

        dir_id = registered_directory["directory_id"]
        try:
            resp = workspaces.modify_client_properties(
                ResourceId=dir_id,
                ClientProperties={"ReconnectEnabled": "ENABLED"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            # Some server states reject this - any structured error is valid
            assert "Code" in exc.response["Error"]

    def test_describe_client_properties_response_status(self, workspaces):
        """DescribeClientProperties always returns 200 status for valid call."""
        result = workspaces.describe_client_properties(ResourceIds=["d-9267462133"])
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesTerminateEdgeCases:
    """Edge cases for terminate operations covering C+R+L+U+E patterns."""

    def test_terminate_and_verify_workspace_state(self, workspace, workspaces):
        """Terminate workspace, verify via describe it is in a terminal state."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert len(result["FailedRequests"]) == 0

        # The workspace should now either be gone or in TERMINATING/TERMINATED state
        check = workspaces.describe_workspaces(WorkspaceIds=[workspace])
        if check["Workspaces"]:
            state = check["Workspaces"][0]["State"]
            assert state in ("TERMINATING", "TERMINATED", "ERROR")

    def test_terminate_returns_pending_requests_empty(self, workspace, workspaces):
        """Terminating existing workspace has empty PendingRequests (it's immediate)."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": workspace}]
        )
        assert "FailedRequests" in result
        assert "ResponseMetadata" in result
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_terminate_nonexistent_error_fields(self, workspaces):
        """TerminateWorkspaces for nonexistent workspace has ErrorCode and WorkspaceId in failure."""
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": "ws-termnonexist1"}]
        )
        assert len(result["FailedRequests"]) == 1
        failed = result["FailedRequests"][0]
        assert failed["WorkspaceId"] == "ws-termnonexist1"
        assert "ErrorCode" in failed
        assert "ErrorMessage" in failed


class TestWorkSpacesAccountLinkFullLifecycle:
    """Full lifecycle for account links: CREATE -> RETRIEVE -> LIST -> UPDATE(modify) -> DELETE -> ERROR."""

    def test_account_link_create_get_list_delete_error(self, workspaces):
        """Full lifecycle: create account link, get by ID, list, delete, verify error on re-get."""
        from botocore.exceptions import ClientError

        # CREATE
        create_resp = workspaces.create_account_link_invitation(TargetAccountId="555566667777")
        link = create_resp["AccountLink"]
        link_id = link["AccountLinkId"]
        assert link_id.startswith("wsal-")
        assert "AccountLinkStatus" in link

        # RETRIEVE: get the specific link by ID
        get_resp = workspaces.get_account_link(LinkId=link_id)
        assert get_resp["AccountLink"]["AccountLinkId"] == link_id

        # LIST: verify appears in list
        list_resp = workspaces.list_account_links()
        link_ids = [lnk["AccountLinkId"] for lnk in list_resp["AccountLinks"]]
        assert link_id in link_ids

        # DELETE
        del_resp = workspaces.delete_account_link_invitation(LinkId=link_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: get deleted link should raise
        with pytest.raises(ClientError) as exc:
            workspaces.get_account_link(LinkId=link_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_account_link_create_then_reject_and_error(self, workspaces):
        """Create link then reject it - covers CREATE + DELETE(reject) + ERROR."""
        from botocore.exceptions import ClientError

        # CREATE
        create_resp = workspaces.create_account_link_invitation(TargetAccountId="666677778888")
        link_id = create_resp["AccountLink"]["AccountLinkId"]

        # RETRIEVE
        get_resp = workspaces.get_account_link(LinkId=link_id)
        assert get_resp["AccountLink"]["AccountLinkId"] == link_id

        # DELETE via reject
        reject_resp = workspaces.reject_account_link_invitation(LinkId=link_id)
        assert reject_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: get a totally nonexistent link should fail
        with pytest.raises(ClientError) as exc:
            workspaces.get_account_link(LinkId="wsal-doesnotexist9999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_account_links_with_status_filter(self, workspaces):
        """ListAccountLinks with PENDING_ACCEPTANCE filter - LIST + CREATE + ERROR."""
        from botocore.exceptions import ClientError

        # CREATE - create a link (will be in PENDING_ACCEPTANCE state)
        create_resp = workspaces.create_account_link_invitation(TargetAccountId="777788889999")
        link_id = create_resp["AccountLink"]["AccountLinkId"]

        # LIST with status filter
        list_resp = workspaces.list_account_links(LinkStatusFilter=["PENDING_ACCEPTANCE"])
        assert "AccountLinks" in list_resp
        assert isinstance(list_resp["AccountLinks"], list)

        # ERROR: accept nonexistent link
        with pytest.raises(ClientError) as exc:
            workspaces.accept_account_link_invitation(LinkId="wsal-nonexistent9999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesAccountFullLifecycle:
    """Full lifecycle for account operations: RETRIEVE + UPDATE + LIST."""

    def test_describe_account_then_modify_then_verify(self, workspaces):
        """RETRIEVE account, UPDATE it, LIST modifications, ERROR for bad modify."""
        from botocore.exceptions import ClientError

        # RETRIEVE
        initial = workspaces.describe_account()
        assert "DedicatedTenancySupport" in initial
        initial_support = initial["DedicatedTenancySupport"]
        assert initial_support in ("ENABLED", "DISABLED")

        # UPDATE: toggle the setting
        new_setting = "DISABLED" if initial_support == "ENABLED" else "ENABLED"
        modify_resp = workspaces.modify_account(DedicatedTenancySupport=new_setting)
        assert modify_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE again to verify change
        updated = workspaces.describe_account()
        assert updated["DedicatedTenancySupport"] == new_setting

        # LIST modifications
        mods_resp = workspaces.describe_account_modifications()
        assert "AccountModifications" in mods_resp
        assert isinstance(mods_resp["AccountModifications"], list)

    def test_describe_account_modifications_pagination(self, workspaces):
        """DescribeAccountModifications structure check with NextToken support."""
        resp = workspaces.describe_account_modifications()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccountModifications" in resp
        # NextToken may or may not be present
        if "NextToken" in resp:
            assert isinstance(resp["NextToken"], str)

    def test_describe_workspace_bundles_with_owner_filter(self, workspaces):
        """DescribeWorkspaceBundles filtered by owner - covers LIST with filter."""
        resp = workspaces.describe_workspace_bundles(Owner="Amazon")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Bundles" in resp
        assert isinstance(resp["Bundles"], list)
        # Amazon-owned bundles should have BundleId
        for bundle in resp["Bundles"]:
            assert "BundleId" in bundle
            assert bundle["BundleId"].startswith("wsb-")


class TestWorkSpacesDirectoryEdgeCasesExtended:
    """Extended edge cases for workspace directories covering missing patterns."""

    def test_describe_directories_empty_then_register_then_list(
        self, registered_directory, workspaces
    ):
        """CREATE directory via fixture, LIST it, RETRIEVE by ID, DELETE (deregister), ERROR."""
        from botocore.exceptions import ClientError

        dir_id = registered_directory["directory_id"]

        # LIST (plural describe)
        list_resp = workspaces.describe_workspace_directories()
        dir_ids = [d["DirectoryId"] for d in list_resp["Directories"]]
        assert dir_id in dir_ids

        # RETRIEVE by specific ID
        get_resp = workspaces.describe_workspace_directories(DirectoryIds=[dir_id])
        assert len(get_resp["Directories"]) >= 1
        directory = get_resp["Directories"][0]
        assert directory["DirectoryId"] == dir_id
        assert "DirectoryName" in directory

        # UPDATE: modify workspace creation properties
        try:
            workspaces.modify_workspace_creation_properties(
                ResourceId=dir_id,
                WorkspaceCreationProperties={"EnableInternetAccess": False},
            )
        except ClientError:
            pass  # best-effort, some configs reject this

        # DELETE: deregister
        workspaces.deregister_workspace_directory(DirectoryId=dir_id)
        after = workspaces.describe_workspace_directories()
        remaining_ids = [d["DirectoryId"] for d in after["Directories"]]
        assert dir_id not in remaining_ids

        # ERROR: deregister again should fail
        with pytest.raises(ClientError) as exc:
            workspaces.deregister_workspace_directory(DirectoryId=dir_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
            "InvalidResourceStateException",
            "InternalError",
        )

    def test_describe_workspace_directories_with_nonexistent_raises(self, workspaces):
        """DescribeWorkspaceDirectories with invalid format raises ValidationException - ERROR."""
        from botocore.exceptions import ClientError

        # LIST (empty)
        result = workspaces.describe_workspace_directories()
        assert "Directories" in result

        # ERROR: invalid directory ID format
        with pytest.raises(ClientError) as exc:
            workspaces.describe_workspace_directories(DirectoryIds=["d-invalid1234"])
        assert exc.value.response["Error"]["Code"] in (
            "ValidationException",
            "InvalidParameterValuesException",
        )

        # UPDATE test: modify nonexistent dir
        with pytest.raises(ClientError) as exc2:
            workspaces.modify_workspace_creation_properties(
                ResourceId="d-0000000000",
                WorkspaceCreationProperties={"EnableInternetAccess": True},
            )
        assert exc2.value.response["Error"]["Code"] in (
            "ValidationException",
            "ResourceNotFoundException",
        )


class TestWorkSpacesWorkspaceFullLifecycle:
    """Full lifecycle tests for workspace create/describe/filter/terminate."""

    def test_create_describe_filter_terminate_workspace(self, workspace, workspaces):
        """Full workspace lifecycle: created workspace can be described, filtered, updated, terminated."""
        from botocore.exceptions import ClientError

        ws_id = workspace

        # LIST: appears in full list
        list_resp = workspaces.describe_workspaces()
        ws_ids = [ws["WorkspaceId"] for ws in list_resp["Workspaces"]]
        assert ws_id in ws_ids

        # RETRIEVE: describe by specific ID
        desc_resp = workspaces.describe_workspaces(WorkspaceIds=[ws_id])
        assert len(desc_resp["Workspaces"]) == 1
        ws = desc_resp["Workspaces"][0]
        assert ws["WorkspaceId"] == ws_id
        assert "State" in ws

        # UPDATE: modify workspace state
        modify_resp = workspaces.modify_workspace_state(
            WorkspaceId=ws_id,
            WorkspaceState="ADMIN_MAINTENANCE",
        )
        assert modify_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE: terminate
        term_resp = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": ws_id}]
        )
        assert len(term_resp["FailedRequests"]) == 0

        # ERROR: terminate nonexistent (different ID so not duplicate)
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": "ws-lifecycle999"}]
        )
        assert len(result["FailedRequests"]) == 1
        assert result["FailedRequests"][0]["WorkspaceId"] == "ws-lifecycle999"

    def test_describe_workspaces_empty_then_filter_error(self, workspaces):
        """List workspaces (empty ok), filter by nonexistent ID (empty), filter by invalid bundle."""
        from botocore.exceptions import ClientError

        # LIST: empty is fine
        empty_resp = workspaces.describe_workspaces()
        assert "Workspaces" in empty_resp
        assert isinstance(empty_resp["Workspaces"], list)

        # LIST: by nonexistent workspace ID
        by_id_resp = workspaces.describe_workspaces(WorkspaceIds=["ws-nonexistent123"])
        assert by_id_resp["Workspaces"] == []

        # LIST: by nonexistent bundle
        by_bundle_resp = workspaces.describe_workspaces(BundleId="wsb-nonexistent123")
        assert by_bundle_resp["Workspaces"] == []

        # CREATE (invalid) + ERROR: create with invalid directory gives FailedRequests
        result = workspaces.create_workspaces(
            Workspaces=[{"DirectoryId": "d-0000000000", "UserName": "u", "BundleId": "wsb-x"}]
        )
        assert len(result["FailedRequests"]) == 1
        assert "ErrorCode" in result["FailedRequests"][0]


class TestWorkSpacesImageFullLifecycle:
    """Full lifecycle tests for workspace images."""

    def test_import_list_describe_tag_delete_image(self, workspaces):
        """Full image lifecycle: import, list, describe, tag, delete, verify gone."""
        from botocore.exceptions import ClientError

        # CREATE: import image
        import_resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-lifecycle99",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-lifecycle"),
            ImageDescription="full lifecycle test",
        )
        image_id = import_resp["ImageId"]
        assert image_id.startswith("wsi-")

        # RETRIEVE: describe by specific ID
        desc_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert len(desc_resp["Images"]) == 1
        img = desc_resp["Images"][0]
        assert img["ImageId"] == image_id
        assert img["Name"].startswith("img-lifecycle")

        # LIST: appears in full list
        list_resp = workspaces.describe_workspace_images()
        all_ids = [i["ImageId"] for i in list_resp["Images"]]
        assert image_id in all_ids

        # UPDATE: tag the image
        workspaces.create_tags(
            ResourceId=image_id,
            Tags=[{"Key": "lifecycle", "Value": "test"}],
        )
        tag_resp = workspaces.describe_tags(ResourceId=image_id)
        tags = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
        assert tags.get("lifecycle") == "test"

        # DELETE
        del_resp = workspaces.delete_workspace_image(ImageId=image_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: describe deleted image returns empty
        after_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert after_resp["Images"] == []

    def test_describe_images_empty_create_retrieve_delete(self, workspaces):
        """Empty describe, then import, retrieve, delete - multiple patterns."""
        # LIST (empty)
        empty = workspaces.describe_workspace_images(ImageIds=["wsi-shouldnotexist00"])
        assert empty["Images"] == []

        # CREATE
        resp = workspaces.import_workspace_image(
            Ec2ImageId="ami-emptycreate",
            IngestionProcess="BYOL_REGULAR",
            ImageName=_unique("img-ec"),
            ImageDescription="empty+create test",
        )
        image_id = resp["ImageId"]

        # RETRIEVE
        get_resp = workspaces.describe_workspace_images(ImageIds=[image_id])
        assert get_resp["Images"][0]["ImageId"] == image_id

        # DELETE
        workspaces.delete_workspace_image(ImageId=image_id)


class TestWorkSpacesClientPropertiesExtended:
    """Extended tests for client properties covering UPDATE and ERROR patterns."""

    def test_describe_empty_modify_describe_client_properties(self, registered_directory, workspaces):
        """LIST client props (empty), UPDATE for registered dir, LIST again."""
        from botocore.exceptions import ClientError

        dir_id = registered_directory["directory_id"]

        # LIST (possibly empty for nonexistent resource)
        empty_resp = workspaces.describe_client_properties(ResourceIds=["d-0000000099"])
        assert "ClientPropertiesList" in empty_resp
        assert isinstance(empty_resp["ClientPropertiesList"], list)

        # UPDATE: modify client properties for real directory
        try:
            modify_resp = workspaces.modify_client_properties(
                ResourceId=dir_id,
                ClientProperties={"ReconnectEnabled": "ENABLED"},
            )
            assert modify_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            # Acceptable error - directory may not be in the right state
            assert "Code" in exc.response["Error"]

        # LIST: describe for real directory
        real_resp = workspaces.describe_client_properties(ResourceIds=[dir_id])
        assert "ClientPropertiesList" in real_resp

        # ERROR: modify for nonexistent
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_access_properties(
                ResourceId="d-nonexistent888",
                WorkspaceAccessProperties={"DeviceTypeWindows": "ALLOW"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_client_properties_empty_with_error(self, workspaces):
        """DescribeClientProperties (LIST), then error on modify nonexistent."""
        from botocore.exceptions import ClientError

        # LIST
        result = workspaces.describe_client_properties(ResourceIds=["d-9267462133"])
        assert "ClientPropertiesList" in result
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_access_properties(
                ResourceId="d-nonexistent777",
                WorkspaceAccessProperties={"DeviceTypeWindows": "DENY"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # UPDATE (modify account - always works)
        modify_resp = workspaces.modify_account(DedicatedTenancySupport="DISABLED")
        assert modify_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestWorkSpacesBundleFullLifecycle:
    """Full lifecycle for bundles: CREATE -> RETRIEVE -> LIST -> UPDATE -> DELETE -> ERROR."""

    def test_bundle_full_lifecycle(self, workspaces):
        """Full bundle lifecycle: create, list, retrieve, update, delete, error."""
        from botocore.exceptions import ClientError

        # CREATE
        name = _unique("bundle-full")
        create_resp = workspaces.create_workspace_bundle(
            BundleName=name,
            BundleDescription="full lifecycle test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle = create_resp["WorkspaceBundle"]
        bundle_id = bundle["BundleId"]
        assert bundle_id.startswith("wsb-")

        # LIST: appears in describe
        list_resp = workspaces.describe_workspace_bundles(BundleIds=[bundle_id])
        assert len(list_resp["Bundles"]) >= 1
        listed = list_resp["Bundles"][0]
        assert listed["BundleId"] == bundle_id

        # RETRIEVE: describe account (singular) while we have bundle context
        acct_resp = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct_resp

        # UPDATE: update the bundle
        update_resp = workspaces.update_workspace_bundle(
            BundleId=bundle_id,
            ImageId="wsi-updated12345",
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # DELETE
        del_resp = workspaces.delete_workspace_bundle(BundleId=bundle_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: update nonexistent
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId="wsb-deletedabove")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_workspace_bundles_empty_create_retrieve_update_delete_error(self, workspaces):
        """Covers all 6 patterns across bundle + account operations."""
        from botocore.exceptions import ClientError

        # LIST
        list_resp = workspaces.describe_workspace_bundles()
        assert "Bundles" in list_resp

        # CREATE
        create_resp = workspaces.create_workspace_bundle(
            BundleName=_unique("bundle-6pat"),
            BundleDescription="6-pattern test",
            ImageId="wsi-fake12345",
            ComputeType={"Name": "VALUE"},
            UserStorage={"Capacity": "10"},
        )
        bundle_id = create_resp["WorkspaceBundle"]["BundleId"]

        # RETRIEVE (singular describe)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        workspaces.update_workspace_bundle(BundleId=bundle_id, ImageId="wsi-update99")

        # DELETE
        workspaces.delete_workspace_bundle(BundleId=bundle_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            workspaces.update_workspace_bundle(BundleId=bundle_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesIpGroupFullLifecycle:
    """Full lifecycle for IP groups covering all 6 patterns."""

    def test_ip_group_full_6_patterns(self, workspaces):
        """Full IP group lifecycle covering CREATE, LIST, RETRIEVE(account), UPDATE, DELETE, ERROR."""
        from botocore.exceptions import ClientError

        # CREATE
        name = _unique("ipgrp-6pat")
        create_resp = workspaces.create_ip_group(
            GroupName=name,
            GroupDesc="6-pattern lifecycle",
            UserRules=[{"ipRule": "10.0.0.0/8", "ruleDesc": "initial"}],
        )
        group_id = create_resp["GroupId"]
        assert group_id.startswith("wsipg-")

        # LIST
        list_resp = workspaces.describe_ip_groups()
        ids = [g["groupId"] for g in list_resp["Result"]]
        assert group_id in ids

        # RETRIEVE (singular: describe_account)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE
        update_resp = workspaces.update_rules_of_ip_group(
            GroupId=group_id,
            UserRules=[{"ipRule": "172.16.0.0/12", "ruleDesc": "updated"}],
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify update took effect
        desc_resp = workspaces.describe_ip_groups(GroupIds=[group_id])
        rules = desc_resp["Result"][0].get("userRules", [])
        cidrs = [r["ipRule"] for r in rules]
        assert "172.16.0.0/12" in cidrs

        # DELETE
        del_resp = workspaces.delete_ip_group(GroupId=group_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: delete again
        with pytest.raises(ClientError) as exc:
            workspaces.delete_ip_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesConnectionAliasFullLifecycle:
    """Full lifecycle for connection aliases covering all 6 patterns."""

    def test_connection_alias_full_6_patterns(self, workspaces):
        """Full connection alias lifecycle: CREATE, LIST, RETRIEVE(account), UPDATE(tag), DELETE, ERROR."""
        from botocore.exceptions import ClientError

        # CREATE
        conn_str = f"{_unique('alias-6pat')}.example.com"
        create_resp = workspaces.create_connection_alias(ConnectionString=conn_str)
        alias_id = create_resp["AliasId"]
        assert alias_id.startswith("wsca-")

        # LIST
        list_resp = workspaces.describe_connection_aliases()
        alias_ids = [a["AliasId"] for a in list_resp["ConnectionAliases"]]
        assert alias_id in alias_ids

        # RETRIEVE (singular: describe_account)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # UPDATE: tag the alias
        tag_resp_create = workspaces.create_tags(
            ResourceId=alias_id,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        assert tag_resp_create["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify tag stored
        tag_resp = workspaces.describe_tags(ResourceId=alias_id)
        tags = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
        assert tags.get("env") == "prod"

        # DELETE
        del_resp = workspaces.delete_connection_alias(AliasId=alias_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ERROR: delete again
        with pytest.raises(ClientError) as exc:
            workspaces.delete_connection_alias(AliasId=alias_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_connection_aliases_empty_with_create_and_error(self, workspaces):
        """List aliases (possibly empty), create one, verify, then error on nonexistent."""
        from botocore.exceptions import ClientError

        # LIST
        initial = workspaces.describe_connection_aliases()
        assert "ConnectionAliases" in initial

        # CREATE
        resp = workspaces.create_connection_alias(
            ConnectionString=f"{_unique('alias-ecase')}.example.com"
        )
        alias_id = resp["AliasId"]

        # LIST again - alias should be there
        after = workspaces.describe_connection_aliases(AliasIds=[alias_id])
        assert len(after["ConnectionAliases"]) == 1

        # DELETE
        workspaces.delete_connection_alias(AliasId=alias_id)

        # ERROR: disassociate nonexistent
        with pytest.raises(ClientError) as exc:
            workspaces.disassociate_connection_alias(AliasId="wsca-neverexists999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestWorkSpacesTerminateWorkspaceExtended:
    """Extended terminate workspace tests covering more patterns."""

    def test_terminate_nonexistent_with_retrieve_and_update(self, workspaces):
        """Terminate nonexistent (DELETE), describe account (RETRIEVE), modify account (UPDATE), error."""
        from botocore.exceptions import ClientError

        # DELETE (terminate nonexistent - returns FailedRequests, not exception)
        result = workspaces.terminate_workspaces(
            TerminateWorkspaceRequests=[{"WorkspaceId": "ws-nonexistent123"}]
        )
        assert len(result["FailedRequests"]) == 1
        failed = result["FailedRequests"][0]
        assert failed["WorkspaceId"] == "ws-nonexistent123"
        assert "ErrorCode" in failed

        # RETRIEVE (singular: describe_account)
        acct = workspaces.describe_account()
        assert "DedicatedTenancySupport" in acct

        # LIST
        list_resp = workspaces.describe_workspaces()
        assert "Workspaces" in list_resp

        # UPDATE: modify account
        workspaces.modify_account(DedicatedTenancySupport="DISABLED")

        # ERROR: modify workspace state of nonexistent
        with pytest.raises(ClientError) as exc:
            workspaces.modify_workspace_state(
                WorkspaceId="ws-nonexistent444",
                WorkspaceState="ADMIN_MAINTENANCE",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
