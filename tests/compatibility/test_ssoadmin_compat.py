"""SSO Admin compatibility tests."""

import json
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

    def test_delete_account_assignment(self, ssoadmin, instance_arn, permission_set):
        principal_id = _unique("user")
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        resp = ssoadmin.delete_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        status = resp["AccountAssignmentDeletionStatus"]
        assert status["Status"] == "SUCCEEDED"
        assert status["PrincipalId"] == principal_id

    def test_describe_account_assignment_creation_status(
        self, ssoadmin, instance_arn, permission_set
    ):
        principal_id = _unique("user")
        create_resp = ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        request_id = create_resp["AccountAssignmentCreationStatus"]["RequestId"]
        resp = ssoadmin.describe_account_assignment_creation_status(
            InstanceArn=instance_arn,
            AccountAssignmentCreationRequestId=request_id,
        )
        status = resp["AccountAssignmentCreationStatus"]
        assert status["RequestId"] == request_id
        assert status["Status"] == "SUCCEEDED"

    def test_describe_account_assignment_deletion_status(
        self, ssoadmin, instance_arn, permission_set
    ):
        principal_id = _unique("user")
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        delete_resp = ssoadmin.delete_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        request_id = delete_resp["AccountAssignmentDeletionStatus"]["RequestId"]
        resp = ssoadmin.describe_account_assignment_deletion_status(
            InstanceArn=instance_arn,
            AccountAssignmentDeletionRequestId=request_id,
        )
        status = resp["AccountAssignmentDeletionStatus"]
        assert status["RequestId"] == request_id
        assert status["Status"] == "SUCCEEDED"

    def test_list_account_assignments_for_principal(self, ssoadmin, instance_arn, permission_set):
        principal_id = _unique("user")
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        resp = ssoadmin.list_account_assignments_for_principal(
            InstanceArn=instance_arn,
            PrincipalId=principal_id,
            PrincipalType="USER",
        )
        assignments = resp["AccountAssignments"]
        assert isinstance(assignments, list)
        assert len(assignments) >= 1
        assert assignments[0]["PrincipalId"] == principal_id

    def test_list_accounts_for_provisioned_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        # Create an account assignment and provision so the account appears in the list
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=_unique("user"),
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        ssoadmin.provision_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            TargetType="ALL_PROVISIONED_ACCOUNTS",
        )
        resp = ssoadmin.list_accounts_for_provisioned_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        account_ids = resp["AccountIds"]
        assert len(account_ids) >= 1
        assert account_ids[0] == "123456789012"

    def test_list_permission_sets_provisioned_to_account(
        self, ssoadmin, instance_arn, permission_set
    ):
        # Create an account assignment then provision so the permission set appears provisioned
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=_unique("user"),
            PrincipalType="USER",
            TargetId="111122223333",
            TargetType="AWS_ACCOUNT",
        )
        ssoadmin.provision_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            TargetType="ALL_PROVISIONED_ACCOUNTS",
        )
        resp = ssoadmin.list_permission_sets_provisioned_to_account(
            InstanceArn=instance_arn,
            AccountId="111122223333",
        )
        assert isinstance(resp["PermissionSets"], list)
        assert permission_set["PermissionSetArn"] in resp["PermissionSets"]


class TestSSOAdminManagedPolicies:
    def test_attach_managed_policy_to_permission_set(self, ssoadmin, instance_arn, permission_set):
        ssoadmin.attach_managed_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            ManagedPolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess",
        )
        # verify it's attached by confirming count increased
        listed = ssoadmin.list_managed_policies_in_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        policies = listed["AttachedManagedPolicies"]
        assert len(policies) == 1
        assert policies[0]["Name"] == "ReadOnlyAccess"
        # cleanup
        ssoadmin.detach_managed_policy_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            ManagedPolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess",
        )

    def test_list_managed_policies_in_permission_set(self, ssoadmin, instance_arn, permission_set):
        ssoadmin.attach_managed_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            ManagedPolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess",
        )
        resp = ssoadmin.list_managed_policies_in_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        policies = resp["AttachedManagedPolicies"]
        assert len(policies) == 1
        assert policies[0]["Arn"] == "arn:aws:iam::aws:policy/ReadOnlyAccess"

    def test_detach_managed_policy_from_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        ssoadmin.attach_managed_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            ManagedPolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess",
        )
        resp = ssoadmin.detach_managed_policy_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            ManagedPolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # verify it's gone
        listed = ssoadmin.list_managed_policies_in_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        policy_arns = [p["Arn"] for p in listed["AttachedManagedPolicies"]]
        assert "arn:aws:iam::aws:policy/ReadOnlyAccess" not in policy_arns

    def test_attach_customer_managed_policy_reference_to_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        resp = ssoadmin.attach_customer_managed_policy_reference_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            CustomerManagedPolicyReference={"Name": "my-custom-policy"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # cleanup
        ssoadmin.detach_customer_managed_policy_reference_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            CustomerManagedPolicyReference={"Name": "my-custom-policy"},
        )

    def test_list_customer_managed_policy_references_in_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        ssoadmin.attach_customer_managed_policy_reference_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            CustomerManagedPolicyReference={"Name": "my-custom-policy"},
        )
        resp = ssoadmin.list_customer_managed_policy_references_in_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        refs = resp["CustomerManagedPolicyReferences"]
        assert len(refs) == 1
        assert refs[0]["Name"] == "my-custom-policy"

    def test_detach_customer_managed_policy_reference_from_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        ssoadmin.attach_customer_managed_policy_reference_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            CustomerManagedPolicyReference={"Name": "my-custom-policy"},
        )
        resp = ssoadmin.detach_customer_managed_policy_reference_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            CustomerManagedPolicyReference={"Name": "my-custom-policy"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # verify it's gone
        listed = ssoadmin.list_customer_managed_policy_references_in_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        names = [p["Name"] for p in listed["CustomerManagedPolicyReferences"]]
        assert "my-custom-policy" not in names


class TestSSOAdminInlinePolicy:
    def test_put_inline_policy_to_permission_set(self, ssoadmin, instance_arn, permission_set):
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        ssoadmin.put_inline_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            InlinePolicy=policy_doc,
        )
        # verify the policy was stored by fetching it
        resp = ssoadmin.get_inline_policy_for_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        stored = json.loads(resp["InlinePolicy"])
        assert stored["Version"] == "2012-10-17"

    def test_get_inline_policy_for_permission_set(self, ssoadmin, instance_arn, permission_set):
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        ssoadmin.put_inline_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            InlinePolicy=policy_doc,
        )
        resp = ssoadmin.get_inline_policy_for_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        assert "InlinePolicy" in resp
        parsed = json.loads(resp["InlinePolicy"])
        assert parsed["Version"] == "2012-10-17"

    def test_delete_inline_policy_from_permission_set(self, ssoadmin, instance_arn, permission_set):
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        ssoadmin.put_inline_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            InlinePolicy=policy_doc,
        )
        resp = ssoadmin.delete_inline_policy_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSSOAdminProvisionAndUpdate:
    def test_provision_permission_set(self, ssoadmin, instance_arn, permission_set):
        resp = ssoadmin.provision_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            TargetType="ALL_PROVISIONED_ACCOUNTS",
        )
        status = resp["PermissionSetProvisioningStatus"]
        assert status["Status"] == "SUCCEEDED"
        assert status["PermissionSetArn"] == permission_set["PermissionSetArn"]

    def test_update_permission_set(self, ssoadmin, instance_arn, permission_set):
        resp = ssoadmin.update_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            Description="updated description",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_instance(self, ssoadmin, instance_arn):
        resp = ssoadmin.update_instance(
            InstanceArn=instance_arn,
            Name="updated-instance-name",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
