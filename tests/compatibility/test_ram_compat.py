"""AWS RAM (Resource Access Manager) compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def ram():
    return make_client("ram")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def resource_share(ram):
    """Create a resource share and delete it after the test."""
    name = _unique("compat-share")
    resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
    share = resp["resourceShare"]
    yield share
    try:
        ram.delete_resource_share(resourceShareArn=share["resourceShareArn"])
    except Exception:
        pass


class TestRAMResourceShareLifecycle:
    def test_get_resource_shares_empty(self, ram):
        """get_resource_shares returns a list (possibly with prior shares)."""
        resp = ram.get_resource_shares(resourceOwner="SELF")
        assert "resourceShares" in resp
        assert isinstance(resp["resourceShares"], list)

    def test_create_resource_share(self, ram):
        name = _unique("create-test")
        resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
        share = resp["resourceShare"]
        assert share["name"] == name
        assert share["allowExternalPrincipals"] is True
        assert share["status"] == "ACTIVE"
        assert "resourceShareArn" in share
        assert "owningAccountId" in share
        assert "creationTime" in share
        assert "lastUpdatedTime" in share
        # cleanup
        ram.delete_resource_share(resourceShareArn=share["resourceShareArn"])

    def test_create_resource_share_appears_in_list(self, ram, resource_share):
        resp = ram.get_resource_shares(resourceOwner="SELF")
        arns = [s["resourceShareArn"] for s in resp["resourceShares"]]
        assert resource_share["resourceShareArn"] in arns

    def test_update_resource_share_name(self, ram, resource_share):
        new_name = _unique("updated")
        resp = ram.update_resource_share(
            resourceShareArn=resource_share["resourceShareArn"],
            name=new_name,
        )
        updated = resp["resourceShare"]
        assert updated["name"] == new_name
        assert updated["resourceShareArn"] == resource_share["resourceShareArn"]

    def test_update_resource_share_allow_external(self, ram, resource_share):
        resp = ram.update_resource_share(
            resourceShareArn=resource_share["resourceShareArn"],
            allowExternalPrincipals=False,
        )
        updated = resp["resourceShare"]
        assert updated["allowExternalPrincipals"] is False

    def test_delete_resource_share(self, ram):
        name = _unique("delete-test")
        resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
        arn = resp["resourceShare"]["resourceShareArn"]

        del_resp = ram.delete_resource_share(resourceShareArn=arn)
        assert del_resp["returnValue"] is True

    def test_create_multiple_resource_shares(self, ram):
        name1 = _unique("multi1")
        name2 = _unique("multi2")
        resp1 = ram.create_resource_share(name=name1, allowExternalPrincipals=True)
        resp2 = ram.create_resource_share(name=name2, allowExternalPrincipals=False)
        arn1 = resp1["resourceShare"]["resourceShareArn"]
        arn2 = resp2["resourceShare"]["resourceShareArn"]

        resp = ram.get_resource_shares(resourceOwner="SELF")
        arns = [s["resourceShareArn"] for s in resp["resourceShares"]]
        assert arn1 in arns
        assert arn2 in arns

        # cleanup
        ram.delete_resource_share(resourceShareArn=arn1)
        ram.delete_resource_share(resourceShareArn=arn2)


class TestRAMResourceShareAssociations:
    def test_get_resource_share_associations_principal(self, ram):
        """GetResourceShareAssociations returns associations list for PRINCIPAL type."""
        resp = ram.get_resource_share_associations(associationType="PRINCIPAL")
        assert "resourceShareAssociations" in resp
        assert isinstance(resp["resourceShareAssociations"], list)

    def test_get_resource_share_associations_resource(self, ram):
        """GetResourceShareAssociations returns associations list for RESOURCE type."""
        resp = ram.get_resource_share_associations(associationType="RESOURCE")
        assert "resourceShareAssociations" in resp
        assert isinstance(resp["resourceShareAssociations"], list)

    def test_get_resource_share_associations_with_share(self, ram, resource_share):
        """GetResourceShareAssociations filtered by resource share ARN."""
        resp = ram.get_resource_share_associations(
            associationType="PRINCIPAL",
            resourceShareArns=[resource_share["resourceShareArn"]],
        )
        assert "resourceShareAssociations" in resp
        assert isinstance(resp["resourceShareAssociations"], list)


class TestRAMListOperations:
    """Tests for RAM list/get operations that require no setup."""

    def test_list_permissions(self, ram):
        """ListPermissions returns a permissions list."""
        resp = ram.list_permissions()
        assert "permissions" in resp
        assert isinstance(resp["permissions"], list)

    def test_list_resource_types(self, ram):
        """ListResourceTypes returns a resourceTypes list."""
        resp = ram.list_resource_types()
        assert "resourceTypes" in resp
        assert isinstance(resp["resourceTypes"], list)

    def test_get_resource_policies(self, ram):
        """GetResourcePolicies returns a policies list."""
        resp = ram.get_resource_policies(
            resourceArns=["arn:aws:ec2:us-east-1:123456789012:vpc/vpc-12345"]
        )
        assert "policies" in resp
        assert isinstance(resp["policies"], list)

    def test_get_resource_share_invitations(self, ram):
        """GetResourceShareInvitations returns an invitations list."""
        resp = ram.get_resource_share_invitations()
        assert "resourceShareInvitations" in resp
        assert isinstance(resp["resourceShareInvitations"], list)

    def test_list_permission_associations(self, ram):
        """ListPermissionAssociations returns an associations list."""
        resp = ram.list_permission_associations()
        assert "permissions" in resp
        assert isinstance(resp["permissions"], list)

    def test_list_replace_permission_associations_work(self, ram):
        """ListReplacePermissionAssociationsWork returns a work items list."""
        resp = ram.list_replace_permission_associations_work()
        assert "replacePermissionAssociationsWorks" in resp
        assert isinstance(resp["replacePermissionAssociationsWorks"], list)

    def test_list_principals_self(self, ram):
        """ListPrincipals with resourceOwner=SELF returns principals list."""
        resp = ram.list_principals(resourceOwner="SELF")
        assert "principals" in resp
        assert isinstance(resp["principals"], list)

    def test_list_resources_self(self, ram):
        """ListResources with resourceOwner=SELF returns resources list."""
        resp = ram.list_resources(resourceOwner="SELF")
        assert "resources" in resp
        assert isinstance(resp["resources"], list)


class TestRAMResourceShareExtended:
    """Extended resource share lifecycle tests: tag, list permissions, etc."""

    def test_tag_and_untag_resource_share(self, ram, resource_share):
        """TagResource and UntagResource on a resource share succeed."""
        arn = resource_share["resourceShareArn"]
        tag_resp = ram.tag_resource(
            resourceShareArn=arn,
            tags=[{"key": "env", "value": "test"}],
        )
        assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        untag_resp = ram.untag_resource(resourceShareArn=arn, tagKeys=["env"])
        assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_resource_share_permissions(self, ram, resource_share):
        """ListResourceSharePermissions returns permissions for a share."""
        arn = resource_share["resourceShareArn"]
        resp = ram.list_resource_share_permissions(resourceShareArn=arn)
        assert "permissions" in resp
        assert isinstance(resp["permissions"], list)


class TestRAMErrorHandling:
    """Tests for operations that raise expected errors with fake ARNs."""

    FAKE_SHARE_ARN = (
        "arn:aws:ram:us-east-1:123456789012:resource-share/00000000-0000-0000-0000-000000000000"
    )
    FAKE_PERMISSION_ARN = (
        "arn:aws:ram::123456789012:permission/00000000-0000-0000-0000-000000000000"
    )

    def test_delete_resource_share_unknown(self, ram):
        """DeleteResourceShare with fake ARN raises UnknownResourceException."""
        with pytest.raises(ClientError) as exc_info:
            ram.delete_resource_share(resourceShareArn=self.FAKE_SHARE_ARN)
        assert exc_info.value.response["Error"]["Code"] == "UnknownResourceException"

    def test_get_permission_unknown(self, ram):
        """GetPermission with fake ARN raises UnknownResourceException."""
        with pytest.raises(ClientError) as exc_info:
            ram.get_permission(permissionArn=self.FAKE_PERMISSION_ARN)
        assert exc_info.value.response["Error"]["Code"] == "UnknownResourceException"

    def test_update_resource_share_unknown(self, ram):
        """UpdateResourceShare with fake ARN raises UnknownResourceException."""
        with pytest.raises(ClientError) as exc_info:
            ram.update_resource_share(resourceShareArn=self.FAKE_SHARE_ARN, name="nope")
        assert exc_info.value.response["Error"]["Code"] == "UnknownResourceException"

    def test_reject_resource_share_invitation_unknown(self, ram):
        """RejectResourceShareInvitation with fake ARN raises UnknownResourceException."""
        fake_invitation_arn = (
            "arn:aws:ram:us-east-1:123456789012:"
            "resource-share-invitation/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            ram.reject_resource_share_invitation(resourceShareInvitationArn=fake_invitation_arn)
        assert exc_info.value.response["Error"]["Code"] == "UnknownResourceException"

    def test_list_permission_versions_unknown(self, ram):
        """ListPermissionVersions with fake ARN raises UnknownResourceException."""
        with pytest.raises(ClientError) as exc_info:
            ram.list_permission_versions(permissionArn=self.FAKE_PERMISSION_ARN)
        assert exc_info.value.response["Error"]["Code"] == "UnknownResourceException"
