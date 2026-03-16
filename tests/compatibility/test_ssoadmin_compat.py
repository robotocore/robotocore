"""SSO Admin compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ssoadmin():
    return make_client("sso-admin")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def instance_arn(ssoadmin):
    """Return the ARN of the default SSO instance."""
    instances = ssoadmin.list_instances()["Instances"]
    assert len(instances) > 0
    return instances[0]["InstanceArn"]


@pytest.fixture
def permission_set(ssoadmin, instance_arn):
    """Create a permission set and clean it up after the test."""
    name = _unique("ps")
    resp = ssoadmin.create_permission_set(
        Name=name,
        InstanceArn=instance_arn,
        Description="compat test permission set",
    )
    ps_arn = resp["PermissionSet"]["PermissionSetArn"]
    yield resp["PermissionSet"]
    try:
        ssoadmin.delete_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
        )
    except Exception:
        pass  # best-effort cleanup


class TestSSOAdminInstances:
    def test_list_instances(self, ssoadmin):
        resp = ssoadmin.list_instances()
        assert "Instances" in resp
        instances = resp["Instances"]
        assert len(instances) >= 1
        inst = instances[0]
        assert "InstanceArn" in inst
        assert "IdentityStoreId" in inst


class TestSSOAdminPermissionSets:
    def test_create_permission_set(self, ssoadmin, instance_arn):
        name = _unique("ps")
        resp = ssoadmin.create_permission_set(
            Name=name,
            InstanceArn=instance_arn,
            Description="test create",
        )
        ps = resp["PermissionSet"]
        assert ps["Name"] == name
        assert "PermissionSetArn" in ps
        # cleanup
        ssoadmin.delete_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps["PermissionSetArn"],
        )

    def test_list_permission_sets(self, ssoadmin, instance_arn, permission_set):
        resp = ssoadmin.list_permission_sets(InstanceArn=instance_arn)
        assert "PermissionSets" in resp
        assert permission_set["PermissionSetArn"] in resp["PermissionSets"]

    def test_describe_permission_set(self, ssoadmin, instance_arn, permission_set):
        resp = ssoadmin.describe_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        ps = resp["PermissionSet"]
        assert ps["Name"] == permission_set["Name"]
        assert ps["PermissionSetArn"] == permission_set["PermissionSetArn"]

    def test_delete_permission_set(self, ssoadmin, instance_arn):
        name = _unique("ps-del")
        create_resp = ssoadmin.create_permission_set(
            Name=name,
            InstanceArn=instance_arn,
            Description="to be deleted",
        )
        ps_arn = create_resp["PermissionSet"]["PermissionSetArn"]
        ssoadmin.delete_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
        )
        # Verify it's gone
        listed = ssoadmin.list_permission_sets(InstanceArn=instance_arn)
        assert ps_arn not in listed.get("PermissionSets", [])


class TestSSOAdminAccountAssignments:
    def test_create_account_assignment(self, ssoadmin, instance_arn, permission_set):
        resp = ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=_unique("user"),
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        status = resp["AccountAssignmentCreationStatus"]
        assert status["Status"] == "SUCCEEDED"

    def test_list_account_assignments(self, ssoadmin, instance_arn, permission_set):
        principal_id = _unique("user")
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        resp = ssoadmin.list_account_assignments(
            InstanceArn=instance_arn,
            AccountId="123456789012",
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        assignments = resp["AccountAssignments"]
        principal_ids = [a["PrincipalId"] for a in assignments]
        assert principal_id in principal_ids
