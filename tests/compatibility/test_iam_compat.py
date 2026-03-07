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

    def test_get_account_authorization_details_filter(self, iam):
        """Filter by entity type (User only)."""
        user_name = _unique("authf-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.get_account_authorization_details(Filter=["User"])
            user_names = [u["UserName"] for u in resp.get("UserDetailList", [])]
            assert user_name in user_names
            # When filtering by User, RoleDetailList should be empty
            assert len(resp.get("RoleDetailList", [])) == 0
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMInstanceProfileExtended:
    def test_create_and_delete_instance_profile(self, iam):
        """Create and delete an instance profile without a role."""
        profile_name = _unique("ip-bare")
        resp = iam.create_instance_profile(InstanceProfileName=profile_name)
        assert resp["InstanceProfile"]["InstanceProfileName"] == profile_name
        assert resp["InstanceProfile"]["Roles"] == []
        iam.delete_instance_profile(InstanceProfileName=profile_name)

    def test_list_instance_profiles(self, iam):
        """List instance profiles includes the one we created."""
        profile_name = _unique("ip-list")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        try:
            resp = iam.list_instance_profiles()
            names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
            assert profile_name in names
        finally:
            iam.delete_instance_profile(InstanceProfileName=profile_name)

    def test_add_remove_role_from_instance_profile(self, iam):
        """Add a role, verify, remove it, verify empty."""
        profile_name = _unique("ip-role")
        role_name = _unique("ip-r")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            assert any(r["RoleName"] == role_name for r in resp["InstanceProfile"]["Roles"])

            iam.remove_role_from_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            assert len(resp["InstanceProfile"]["Roles"]) == 0
        finally:
            iam.delete_instance_profile(InstanceProfileName=profile_name)
            iam.delete_role(RoleName=role_name)


class TestIAMLoginProfile:
    def test_create_and_delete_login_profile(self, iam):
        user_name = _unique("login-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.create_login_profile(
                UserName=user_name, Password="TestPass123!", PasswordResetRequired=False
            )
            assert resp["LoginProfile"]["UserName"] == user_name

            get_resp = iam.get_login_profile(UserName=user_name)
            assert get_resp["LoginProfile"]["UserName"] == user_name

            iam.delete_login_profile(UserName=user_name)
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMAccessKeys:
    def test_create_list_delete_access_key(self, iam):
        user_name = _unique("ak-user")
        iam.create_user(UserName=user_name)
        try:
            create_resp = iam.create_access_key(UserName=user_name)
            key_id = create_resp["AccessKey"]["AccessKeyId"]
            assert create_resp["AccessKey"]["UserName"] == user_name
            assert "SecretAccessKey" in create_resp["AccessKey"]

            list_resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in list_resp["AccessKeyMetadata"]]
            assert key_id in key_ids

            iam.delete_access_key(UserName=user_name, AccessKeyId=key_id)
            list_resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in list_resp["AccessKeyMetadata"]]
            assert key_id not in key_ids
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMAccountAlias:
    def test_create_list_delete_account_alias(self, iam):
        alias = _unique("myalias").lower().replace("_", "")
        iam.create_account_alias(AccountAlias=alias)
        try:
            resp = iam.list_account_aliases()
            assert alias in resp["AccountAliases"]
        finally:
            iam.delete_account_alias(AccountAlias=alias)
        resp = iam.list_account_aliases()
        assert alias not in resp["AccountAliases"]


class TestIAMAttachedPolicies:
    def test_list_attached_role_policies(self, iam):
        role_name = _unique("arp-role")
        policy_name = _unique("arp-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role_name)
            attached = [p["PolicyName"] for p in resp["AttachedPolicies"]]
            assert policy_name in attached
        finally:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)

    def test_attach_detach_user_policy(self, iam):
        user_name = _unique("aup-user")
        policy_name = _unique("aup-pol")
        iam.create_user(UserName=user_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_user_policy(UserName=user_name, PolicyArn=arn)
            resp = iam.list_attached_user_policies(UserName=user_name)
            attached = [p["PolicyName"] for p in resp["AttachedPolicies"]]
            assert policy_name in attached

            iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            resp = iam.list_attached_user_policies(UserName=user_name)
            assert len(resp["AttachedPolicies"]) == 0
        finally:
            iam.delete_policy(PolicyArn=arn)
            iam.delete_user(UserName=user_name)


class TestIAMGroupPolicies:
    def test_put_list_delete_group_policy(self, iam):
        group_name = _unique("gp-group")
        policy_name = _unique("gp-pol")
        iam.create_group(GroupName=group_name)
        try:
            iam.put_group_policy(
                GroupName=group_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.list_group_policies(GroupName=group_name)
            assert policy_name in resp["PolicyNames"]

            iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            resp = iam.list_group_policies(GroupName=group_name)
            assert policy_name not in resp["PolicyNames"]
        finally:
            iam.delete_group(GroupName=group_name)


class TestIAMPolicyVersionsExtended:
    def test_create_list_set_default_delete_version(self, iam):
        """Full lifecycle: create v2, set v2 default, delete v1."""
        policy_name = _unique("pvx-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            new_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            v2 = iam.create_policy_version(
                PolicyArn=arn, PolicyDocument=new_doc, SetAsDefault=True
            )
            assert v2["PolicyVersion"]["VersionId"] == "v2"
            assert v2["PolicyVersion"]["IsDefaultVersion"] is True

            versions = iam.list_policy_versions(PolicyArn=arn)
            ids = [v["VersionId"] for v in versions["Versions"]]
            assert "v1" in ids
            assert "v2" in ids

            # v1 is no longer default, delete it
            iam.delete_policy_version(PolicyArn=arn, VersionId="v1")
            versions = iam.list_policy_versions(PolicyArn=arn)
            ids = [v["VersionId"] for v in versions["Versions"]]
            assert "v1" not in ids
            assert "v2" in ids
        finally:
            # Clean remaining non-default versions
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
            iam.delete_policy(PolicyArn=arn)


class TestIAMTagRole:
    def test_tag_untag_role(self, iam):
        role_name = _unique("tag-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.tag_role(
                RoleName=role_name,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            resp = iam.list_role_tags(RoleName=role_name)
            tag_keys = {t["Key"] for t in resp["Tags"]}
            assert "env" in tag_keys
            assert "team" in tag_keys

            iam.untag_role(RoleName=role_name, TagKeys=["team"])
            resp = iam.list_role_tags(RoleName=role_name)
            tag_keys = {t["Key"] for t in resp["Tags"]}
            assert "env" in tag_keys
            assert "team" not in tag_keys
        finally:
            iam.delete_role(RoleName=role_name)


class TestIAMTagUser:
    def test_tag_untag_user(self, iam):
        user_name = _unique("tag-user")
        iam.create_user(UserName=user_name)
        try:
            iam.tag_user(
                UserName=user_name,
                Tags=[
                    {"Key": "dept", "Value": "engineering"},
                    {"Key": "project", "Value": "robotocore"},
                ],
            )
            resp = iam.list_user_tags(UserName=user_name)
            tag_keys = {t["Key"] for t in resp["Tags"]}
            assert "dept" in tag_keys
            assert "project" in tag_keys

            iam.untag_user(UserName=user_name, TagKeys=["project"])
            resp = iam.list_user_tags(UserName=user_name)
            tag_keys = {t["Key"] for t in resp["Tags"]}
            assert "dept" in tag_keys
            assert "project" not in tag_keys
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMGetRolePolicy:
    def test_put_and_get_role_policy(self, iam):
        role_name = _unique("grp-role")
        policy_name = _unique("grp-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            assert resp["RoleName"] == role_name
            assert resp["PolicyName"] == policy_name
            # PolicyDocument comes back URL-encoded from AWS/Moto
            assert "Statement" in resp["PolicyDocument"] or "Statement" in json.loads(
                resp["PolicyDocument"]
            )
        finally:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            iam.delete_role(RoleName=role_name)

    def test_list_role_policies(self, iam):
        role_name = _unique("lrp-role")
        policy_name = _unique("lrp-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.list_role_policies(RoleName=role_name)
            assert policy_name in resp["PolicyNames"]
        finally:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            iam.delete_role(RoleName=role_name)


class TestIAMServiceLinkedRole:
    def test_create_service_linked_role(self, iam):
        """Create a service-linked role for elasticbeanstalk."""
        try:
            resp = iam.create_service_linked_role(
                AWSServiceName="elasticbeanstalk.amazonaws.com",
            )
            role_name = resp["Role"]["RoleName"]
            assert "elasticbeanstalk" in role_name.lower() or "AWSServiceRoleFor" in role_name
        finally:
            # Service-linked roles need deletion via delete_service_linked_role
            iam.delete_service_linked_role(RoleName=role_name)


class TestIAMGetPolicy:
    def test_get_policy(self, iam):
        """Get policy returns full details."""
        policy_name = _unique("gp-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy(PolicyArn=arn)
            assert resp["Policy"]["PolicyName"] == policy_name
            assert resp["Policy"]["Arn"] == arn
            assert resp["Policy"]["IsAttachable"] is True
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_get_policy_version(self, iam):
        """Get a specific policy version document."""
        policy_name = _unique("gpv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy_version(PolicyArn=arn, VersionId="v1")
            assert resp["PolicyVersion"]["VersionId"] == "v1"
            assert resp["PolicyVersion"]["IsDefaultVersion"] is True
        finally:
            iam.delete_policy(PolicyArn=arn)
