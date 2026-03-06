"""IAM compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def iam():
    return make_client("iam")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)

SIMPLE_POLICY_DOC = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
    }
)


class TestIAMUserOperations:
    def test_create_user(self, iam):
        response = iam.create_user(UserName="test-user")
        assert response["User"]["UserName"] == "test-user"
        iam.delete_user(UserName="test-user")

    def test_list_users(self, iam):
        iam.create_user(UserName="list-user-1")
        response = iam.list_users()
        names = [u["UserName"] for u in response["Users"]]
        assert "list-user-1" in names
        iam.delete_user(UserName="list-user-1")

    def test_get_user(self, iam):
        iam.create_user(UserName="get-user")
        response = iam.get_user(UserName="get-user")
        assert response["User"]["UserName"] == "get-user"
        iam.delete_user(UserName="get-user")


class TestIAMRoleOperations:
    def test_create_role(self, iam):
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        response = iam.create_role(RoleName="test-role", AssumeRolePolicyDocument=trust)
        assert response["Role"]["RoleName"] == "test-role"
        iam.delete_role(RoleName="test-role")

    def test_list_roles(self, iam):
        trust = json.dumps({"Version": "2012-10-17", "Statement": []})
        iam.create_role(RoleName="list-role", AssumeRolePolicyDocument=trust)
        response = iam.list_roles()
        names = [r["RoleName"] for r in response["Roles"]]
        assert "list-role" in names
        iam.delete_role(RoleName="list-role")


class TestIAMPolicyOperations:
    def test_create_policy(self, iam):
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        response = iam.create_policy(PolicyName="test-policy", PolicyDocument=policy_doc)
        assert response["Policy"]["PolicyName"] == "test-policy"
        iam.delete_policy(PolicyArn=response["Policy"]["Arn"])

    def test_attach_role_policy(self, iam):
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        iam.create_role(RoleName="attach-role", AssumeRolePolicyDocument=trust)
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        pol = iam.create_policy(PolicyName="attach-policy", PolicyDocument=policy_doc)
        arn = pol["Policy"]["Arn"]
        iam.attach_role_policy(RoleName="attach-role", PolicyArn=arn)
        response = iam.list_attached_role_policies(RoleName="attach-role")
        assert any(p["PolicyArn"] == arn for p in response["AttachedPolicies"])
        iam.detach_role_policy(RoleName="attach-role", PolicyArn=arn)
        iam.delete_policy(PolicyArn=arn)
        iam.delete_role(RoleName="attach-role")


class TestIAMInlinePolicy:
    def test_create_and_attach_inline_policy_to_user(self, iam):
        user_name = _unique("inline-user")
        policy_name = _unique("inline-pol")
        iam.create_user(UserName=user_name)
        try:
            iam.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            response = iam.list_user_policies(UserName=user_name)
            assert policy_name in response["PolicyNames"]

            get_resp = iam.get_user_policy(UserName=user_name, PolicyName=policy_name)
            assert get_resp["PolicyName"] == policy_name
        finally:
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)
            iam.delete_user(UserName=user_name)


class TestIAMInstanceProfile:
    def test_create_instance_profile_and_add_role(self, iam):
        profile_name = _unique("inst-prof")
        role_name = _unique("prof-role")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name,
            )
            resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            roles = resp["InstanceProfile"]["Roles"]
            assert any(r["RoleName"] == role_name for r in roles)
        finally:
            iam.remove_role_from_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name,
            )
            iam.delete_instance_profile(InstanceProfileName=profile_name)
            iam.delete_role(RoleName=role_name)


class TestIAMGroupOperations:
    def test_create_group_and_add_user(self, iam):
        group_name = _unique("test-group")
        user_name = _unique("group-user")
        iam.create_group(GroupName=group_name)
        iam.create_user(UserName=user_name)
        try:
            iam.add_user_to_group(GroupName=group_name, UserName=user_name)
            resp = iam.get_group(GroupName=group_name)
            user_names = [u["UserName"] for u in resp["Users"]]
            assert user_name in user_names
        finally:
            iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            iam.delete_user(UserName=user_name)
            iam.delete_group(GroupName=group_name)


class TestIAMListPolicies:
    def test_list_policies_includes_customer_managed(self, iam):
        policy_name = _unique("list-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policies(Scope="Local")
            names = [p["PolicyName"] for p in resp["Policies"]]
            assert policy_name in names
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_list_policies_all_scope(self, iam):
        """List policies with default (All) scope returns at least our customer policy."""
        policy_name = _unique("scope-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policies(Scope="All")
            names = [p["PolicyName"] for p in resp["Policies"]]
            assert policy_name in names
        finally:
            iam.delete_policy(PolicyArn=arn)


class TestIAMPolicyVersions:
    def test_create_policy_version(self, iam):
        policy_name = _unique("ver-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            new_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            ver_resp = iam.create_policy_version(
                PolicyArn=arn,
                PolicyDocument=new_doc,
                SetAsDefault=True,
            )
            assert ver_resp["PolicyVersion"]["VersionId"] == "v2"
            assert ver_resp["PolicyVersion"]["IsDefaultVersion"] is True

            versions = iam.list_policy_versions(PolicyArn=arn)
            assert len(versions["Versions"]) == 2
        finally:
            # Delete non-default version before deleting policy
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
            iam.delete_policy(PolicyArn=arn)


class TestIAMAccountAuthorizationDetails:
    def test_get_account_authorization_details(self, iam):
        user_name = _unique("auth-user")
        role_name = _unique("auth-role")
        iam.create_user(UserName=user_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.get_account_authorization_details()
            user_names = [u["UserName"] for u in resp.get("UserDetailList", [])]
            role_names = [r["RoleName"] for r in resp.get("RoleDetailList", [])]
            assert user_name in user_names
            assert role_name in role_names
        finally:
            iam.delete_user(UserName=user_name)
            iam.delete_role(RoleName=role_name)
