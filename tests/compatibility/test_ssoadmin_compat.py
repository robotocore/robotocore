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


class TestSSOAdminTagging:
    def test_tag_and_list_tags_for_resource(self, ssoadmin, instance_arn, permission_set):
        ps_arn = permission_set["PermissionSetArn"]
        ssoadmin.tag_resource(
            InstanceArn=instance_arn,
            ResourceArn=ps_arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "owner", "Value": "me"}],
        )
        resp = ssoadmin.list_tags_for_resource(
            InstanceArn=instance_arn,
            ResourceArn=ps_arn,
        )
        tags = resp["Tags"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map["env"] == "test"
        assert tag_map["owner"] == "me"

    def test_untag_resource(self, ssoadmin, instance_arn, permission_set):
        ps_arn = permission_set["PermissionSetArn"]
        ssoadmin.tag_resource(
            InstanceArn=instance_arn,
            ResourceArn=ps_arn,
            Tags=[{"Key": "to-remove", "Value": "yes"}, {"Key": "keep", "Value": "yes"}],
        )
        ssoadmin.untag_resource(
            InstanceArn=instance_arn,
            ResourceArn=ps_arn,
            TagKeys=["to-remove"],
        )
        resp = ssoadmin.list_tags_for_resource(
            InstanceArn=instance_arn,
            ResourceArn=ps_arn,
        )
        keys = [t["Key"] for t in resp["Tags"]]
        assert "to-remove" not in keys
        assert "keep" in keys


class TestSSOAdminPermissionsBoundary:
    def test_put_and_get_permissions_boundary(self, ssoadmin, instance_arn, permission_set):
        ps_arn = permission_set["PermissionSetArn"]
        boundary = {"ManagedPolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess"}
        ssoadmin.put_permissions_boundary_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
            PermissionsBoundary=boundary,
        )
        resp = ssoadmin.get_permissions_boundary_for_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
        )
        assert resp["PermissionsBoundary"]["ManagedPolicyArn"] == boundary["ManagedPolicyArn"]

    def test_delete_permissions_boundary_from_permission_set(
        self, ssoadmin, instance_arn, permission_set
    ):
        ps_arn = permission_set["PermissionSetArn"]
        ssoadmin.put_permissions_boundary_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
            PermissionsBoundary={"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"},
        )
        resp = ssoadmin.delete_permissions_boundary_from_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSSOAdminInstanceCRUD:
    def test_create_and_describe_and_delete_instance(self, ssoadmin):
        resp = ssoadmin.create_instance(Name="my-test-instance")
        instance_arn = resp["InstanceArn"]
        assert instance_arn.startswith("arn:")

        desc = ssoadmin.describe_instance(InstanceArn=instance_arn)
        assert desc["InstanceArn"] == instance_arn

        ssoadmin.delete_instance(InstanceArn=instance_arn)

        instances = ssoadmin.list_instances()["Instances"]
        arns = [i["InstanceArn"] for i in instances]
        assert instance_arn not in arns


class TestSSOAdminInstanceAccessControl:
    def test_create_describe_update_delete_access_control_config(self, ssoadmin, instance_arn):
        access_control_config = {
            "AccessControlAttributes": [
                {
                    "Key": "dept",
                    "Value": {"Source": ["${path:enterprise.department}"]},
                }
            ]
        }
        ssoadmin.create_instance_access_control_attribute_configuration(
            InstanceArn=instance_arn,
            InstanceAccessControlAttributeConfiguration=access_control_config,
        )
        resp = ssoadmin.describe_instance_access_control_attribute_configuration(
            InstanceArn=instance_arn
        )
        config = resp["InstanceAccessControlAttributeConfiguration"]
        assert len(config["AccessControlAttributes"]) == 1
        assert config["AccessControlAttributes"][0]["Key"] == "dept"

        new_config = {
            "AccessControlAttributes": [
                {
                    "Key": "role",
                    "Value": {"Source": ["${path:enterprise.role}"]},
                }
            ]
        }
        ssoadmin.update_instance_access_control_attribute_configuration(
            InstanceArn=instance_arn,
            InstanceAccessControlAttributeConfiguration=new_config,
        )
        resp2 = ssoadmin.describe_instance_access_control_attribute_configuration(
            InstanceArn=instance_arn
        )
        config2 = resp2["InstanceAccessControlAttributeConfiguration"]
        assert config2["AccessControlAttributes"][0]["Key"] == "role"

        ssoadmin.delete_instance_access_control_attribute_configuration(InstanceArn=instance_arn)


class TestSSOAdminApplications:
    def test_create_describe_update_delete_application(self, ssoadmin, instance_arn):
        resp = ssoadmin.create_application(
            ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom",
            InstanceArn=instance_arn,
            Name="my-test-app",
            Description="test application",
            Status="ENABLED",
        )
        app_arn = resp["ApplicationArn"]
        assert app_arn.startswith("arn:")

        desc = ssoadmin.describe_application(ApplicationArn=app_arn)
        assert desc["ApplicationArn"] == app_arn
        assert desc["Name"] == "my-test-app"

        ssoadmin.update_application(
            ApplicationArn=app_arn,
            Name="updated-app-name",
            Status="DISABLED",
        )
        desc2 = ssoadmin.describe_application(ApplicationArn=app_arn)
        assert desc2["Name"] == "updated-app-name"

        apps = ssoadmin.list_applications(InstanceArn=instance_arn)
        app_arns = [a["ApplicationArn"] for a in apps["Applications"]]
        assert app_arn in app_arns

        ssoadmin.delete_application(ApplicationArn=app_arn)
        apps2 = ssoadmin.list_applications(InstanceArn=instance_arn)
        app_arns2 = [a["ApplicationArn"] for a in apps2["Applications"]]
        assert app_arn not in app_arns2

    def test_application_assignment_crud(self, ssoadmin, instance_arn):
        resp = ssoadmin.create_application(
            ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom",
            InstanceArn=instance_arn,
            Name=_unique("app"),
            Status="ENABLED",
        )
        app_arn = resp["ApplicationArn"]
        principal_id = _unique("user")

        ssoadmin.create_application_assignment(
            ApplicationArn=app_arn,
            PrincipalId=principal_id,
            PrincipalType="USER",
        )

        desc = ssoadmin.describe_application_assignment(
            ApplicationArn=app_arn,
            PrincipalId=principal_id,
            PrincipalType="USER",
        )
        assert desc["PrincipalId"] == principal_id

        listed = ssoadmin.list_application_assignments(ApplicationArn=app_arn)
        ids = [a["PrincipalId"] for a in listed["ApplicationAssignments"]]
        assert principal_id in ids

        listed_for_principal = ssoadmin.list_application_assignments_for_principal(
            InstanceArn=instance_arn,
            PrincipalId=principal_id,
            PrincipalType="USER",
        )
        app_arns = [a["ApplicationArn"] for a in listed_for_principal["ApplicationAssignments"]]
        assert app_arn in app_arns

        ssoadmin.delete_application_assignment(
            ApplicationArn=app_arn,
            PrincipalId=principal_id,
            PrincipalType="USER",
        )
        # cleanup
        ssoadmin.delete_application(ApplicationArn=app_arn)

    def test_application_assignment_configuration(self, ssoadmin, instance_arn):
        resp = ssoadmin.create_application(
            ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom",
            InstanceArn=instance_arn,
            Name=_unique("app"),
            Status="ENABLED",
        )
        app_arn = resp["ApplicationArn"]

        ssoadmin.put_application_assignment_configuration(
            ApplicationArn=app_arn,
            AssignmentRequired=True,
        )
        config = ssoadmin.get_application_assignment_configuration(ApplicationArn=app_arn)
        assert config["AssignmentRequired"] is True

        ssoadmin.delete_application(ApplicationArn=app_arn)

    def test_application_access_scope_crud(self, ssoadmin, instance_arn):
        resp = ssoadmin.create_application(
            ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom",
            InstanceArn=instance_arn,
            Name=_unique("app"),
            Status="ENABLED",
        )
        app_arn = resp["ApplicationArn"]

        ssoadmin.put_application_access_scope(
            ApplicationArn=app_arn,
            Scope="openid",
            AuthorizedTargets=["target1"],
        )
        scope = ssoadmin.get_application_access_scope(ApplicationArn=app_arn, Scope="openid")
        assert scope["Scope"] == "openid"

        listed = ssoadmin.list_application_access_scopes(ApplicationArn=app_arn)
        scopes = [s["Scope"] for s in listed["Scopes"]]
        assert "openid" in scopes

        ssoadmin.delete_application_access_scope(ApplicationArn=app_arn, Scope="openid")
        listed2 = ssoadmin.list_application_access_scopes(ApplicationArn=app_arn)
        assert "openid" not in [s["Scope"] for s in listed2["Scopes"]]
        # cleanup
        ssoadmin.delete_application(ApplicationArn=app_arn)

    def test_application_session_configuration(self, ssoadmin, instance_arn):
        resp = ssoadmin.create_application(
            ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom",
            InstanceArn=instance_arn,
            Name=_unique("app"),
            Status="ENABLED",
        )
        app_arn = resp["ApplicationArn"]

        ssoadmin.put_application_session_configuration(
            ApplicationArn=app_arn,
            UserBackgroundSessionApplicationStatus="ENABLED",
        )
        config = ssoadmin.get_application_session_configuration(ApplicationArn=app_arn)
        assert config["UserBackgroundSessionApplicationStatus"] == "ENABLED"
        ssoadmin.delete_application(ApplicationArn=app_arn)


class TestSSOAdminApplicationProviders:
    def test_list_application_providers(self, ssoadmin):
        resp = ssoadmin.list_application_providers()
        assert "ApplicationProviders" in resp
        providers = resp["ApplicationProviders"]
        assert len(providers) >= 1
        assert "ApplicationProviderArn" in providers[0]


class TestSSOAdminStatusLists:
    def test_list_account_assignment_creation_status(self, ssoadmin, instance_arn, permission_set):
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=_unique("user"),
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        resp = ssoadmin.list_account_assignment_creation_status(InstanceArn=instance_arn)
        statuses = resp["AccountAssignmentsCreationStatus"]
        assert isinstance(statuses, list)
        assert len(statuses) >= 1
        assert statuses[0]["Status"] == "SUCCEEDED"

    def test_list_account_assignment_deletion_status(self, ssoadmin, instance_arn, permission_set):
        principal_id = _unique("user")
        ssoadmin.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        ssoadmin.delete_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            PrincipalId=principal_id,
            PrincipalType="USER",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        resp = ssoadmin.list_account_assignment_deletion_status(InstanceArn=instance_arn)
        statuses = resp["AccountAssignmentsDeletionStatus"]
        assert isinstance(statuses, list)
        assert len(statuses) >= 1

    def test_list_permission_set_provisioning_status(self, ssoadmin, instance_arn, permission_set):
        ssoadmin.provision_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set["PermissionSetArn"],
            TargetType="ALL_PROVISIONED_ACCOUNTS",
        )
        resp = ssoadmin.list_permission_set_provisioning_status(InstanceArn=instance_arn)
        statuses = resp["PermissionSetsProvisioningStatus"]
        assert isinstance(statuses, list)
