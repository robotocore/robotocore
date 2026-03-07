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


class TestIAMGroupCRUD:
    """Group create, get, list, delete, add/remove user."""

    def test_create_group(self, iam):
        name = _unique("grp")
        resp = iam.create_group(GroupName=name)
        assert resp["Group"]["GroupName"] == name
        iam.delete_group(GroupName=name)

    def test_get_group(self, iam):
        name = _unique("grp")
        iam.create_group(GroupName=name)
        try:
            resp = iam.get_group(GroupName=name)
            assert resp["Group"]["GroupName"] == name
            assert isinstance(resp["Users"], list)
        finally:
            iam.delete_group(GroupName=name)

    def test_list_groups(self, iam):
        name = _unique("grp")
        iam.create_group(GroupName=name)
        try:
            resp = iam.list_groups()
            names = [g["GroupName"] for g in resp["Groups"]]
            assert name in names
        finally:
            iam.delete_group(GroupName=name)

    def test_delete_group(self, iam):
        name = _unique("grp")
        iam.create_group(GroupName=name)
        iam.delete_group(GroupName=name)
        resp = iam.list_groups()
        names = [g["GroupName"] for g in resp["Groups"]]
        assert name not in names

    def test_add_user_to_group(self, iam):
        group = _unique("grp")
        user = _unique("usr")
        iam.create_group(GroupName=group)
        iam.create_user(UserName=user)
        try:
            iam.add_user_to_group(GroupName=group, UserName=user)
            resp = iam.get_group(GroupName=group)
            user_names = [u["UserName"] for u in resp["Users"]]
            assert user in user_names
        finally:
            iam.remove_user_from_group(GroupName=group, UserName=user)
            iam.delete_user(UserName=user)
            iam.delete_group(GroupName=group)

    def test_remove_user_from_group(self, iam):
        group = _unique("grp")
        user = _unique("usr")
        iam.create_group(GroupName=group)
        iam.create_user(UserName=user)
        iam.add_user_to_group(GroupName=group, UserName=user)
        try:
            iam.remove_user_from_group(GroupName=group, UserName=user)
            resp = iam.get_group(GroupName=group)
            user_names = [u["UserName"] for u in resp["Users"]]
            assert user not in user_names
        finally:
            iam.delete_user(UserName=user)
            iam.delete_group(GroupName=group)


class TestIAMManagedPolicyCRUD:
    """Managed policy create, get, list, attach/detach, get-policy-version."""

    def test_create_managed_policy(self, iam):
        name = _unique("mpol")
        resp = iam.create_policy(PolicyName=name, PolicyDocument=SIMPLE_POLICY_DOC)
        assert resp["Policy"]["PolicyName"] == name
        assert resp["Policy"]["DefaultVersionId"] == "v1"
        iam.delete_policy(PolicyArn=resp["Policy"]["Arn"])

    def test_get_policy(self, iam):
        name = _unique("mpol")
        pol = iam.create_policy(PolicyName=name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy(PolicyArn=arn)
            assert resp["Policy"]["PolicyName"] == name
            assert resp["Policy"]["Arn"] == arn
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_list_policies_local_scope(self, iam):
        name = _unique("mpol")
        pol = iam.create_policy(PolicyName=name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policies(Scope="Local")
            names = [p["PolicyName"] for p in resp["Policies"]]
            assert name in names
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_attach_and_detach_role_policy(self, iam):
        role = _unique("role")
        name = _unique("mpol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role)
            arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
            assert arn in arns

            iam.detach_role_policy(RoleName=role, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role)
            arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
            assert arn not in arns
        finally:
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role)

    def test_get_policy_version(self, iam):
        name = _unique("mpol")
        pol = iam.create_policy(PolicyName=name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy_version(PolicyArn=arn, VersionId="v1")
            assert resp["PolicyVersion"]["VersionId"] == "v1"
            assert resp["PolicyVersion"]["IsDefaultVersion"] is True
            doc = resp["PolicyVersion"]["Document"]
            # Document may be URL-encoded or dict depending on implementation
            assert doc is not None
        finally:
            iam.delete_policy(PolicyArn=arn)


class TestIAMInstanceProfileCRUD:
    """Instance profile create, get, list, add/remove role."""

    def test_create_instance_profile(self, iam):
        name = _unique("ip")
        resp = iam.create_instance_profile(InstanceProfileName=name)
        assert resp["InstanceProfile"]["InstanceProfileName"] == name
        assert resp["InstanceProfile"]["Roles"] == []
        iam.delete_instance_profile(InstanceProfileName=name)

    def test_get_instance_profile(self, iam):
        name = _unique("ip")
        iam.create_instance_profile(InstanceProfileName=name)
        try:
            resp = iam.get_instance_profile(InstanceProfileName=name)
            assert resp["InstanceProfile"]["InstanceProfileName"] == name
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_list_instance_profiles(self, iam):
        name = _unique("ip")
        iam.create_instance_profile(InstanceProfileName=name)
        try:
            resp = iam.list_instance_profiles()
            names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
            assert name in names
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_add_role_to_instance_profile(self, iam):
        ip_name = _unique("ip")
        role_name = _unique("role")
        iam.create_instance_profile(InstanceProfileName=ip_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.add_role_to_instance_profile(
                InstanceProfileName=ip_name, RoleName=role_name
            )
            resp = iam.get_instance_profile(InstanceProfileName=ip_name)
            role_names = [r["RoleName"] for r in resp["InstanceProfile"]["Roles"]]
            assert role_name in role_names
        finally:
            iam.remove_role_from_instance_profile(
                InstanceProfileName=ip_name, RoleName=role_name
            )
            iam.delete_instance_profile(InstanceProfileName=ip_name)
            iam.delete_role(RoleName=role_name)

    def test_remove_role_from_instance_profile(self, iam):
        ip_name = _unique("ip")
        role_name = _unique("role")
        iam.create_instance_profile(InstanceProfileName=ip_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.add_role_to_instance_profile(
            InstanceProfileName=ip_name, RoleName=role_name
        )
        try:
            iam.remove_role_from_instance_profile(
                InstanceProfileName=ip_name, RoleName=role_name
            )
            resp = iam.get_instance_profile(InstanceProfileName=ip_name)
            assert resp["InstanceProfile"]["Roles"] == []
        finally:
            iam.delete_instance_profile(InstanceProfileName=ip_name)
            iam.delete_role(RoleName=role_name)


class TestIAMAccessKeys:
    """Access key create, list, update, delete."""

    def test_create_access_key(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        try:
            resp = iam.create_access_key(UserName=user)
            key = resp["AccessKey"]
            assert key["UserName"] == user
            assert "AccessKeyId" in key
            assert "SecretAccessKey" in key
            assert key["Status"] == "Active"
        finally:
            iam.delete_access_key(
                UserName=user, AccessKeyId=resp["AccessKey"]["AccessKeyId"]
            )
            iam.delete_user(UserName=user)

    def test_list_access_keys(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        key_resp = iam.create_access_key(UserName=user)
        key_id = key_resp["AccessKey"]["AccessKeyId"]
        try:
            resp = iam.list_access_keys(UserName=user)
            key_ids = [k["AccessKeyId"] for k in resp["AccessKeyMetadata"]]
            assert key_id in key_ids
        finally:
            iam.delete_access_key(UserName=user, AccessKeyId=key_id)
            iam.delete_user(UserName=user)

    def test_update_access_key(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        key_resp = iam.create_access_key(UserName=user)
        key_id = key_resp["AccessKey"]["AccessKeyId"]
        try:
            iam.update_access_key(
                UserName=user, AccessKeyId=key_id, Status="Inactive"
            )
            resp = iam.list_access_keys(UserName=user)
            key_meta = [k for k in resp["AccessKeyMetadata"] if k["AccessKeyId"] == key_id][0]
            assert key_meta["Status"] == "Inactive"
        finally:
            iam.delete_access_key(UserName=user, AccessKeyId=key_id)
            iam.delete_user(UserName=user)

    def test_delete_access_key(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        key_resp = iam.create_access_key(UserName=user)
        key_id = key_resp["AccessKey"]["AccessKeyId"]
        iam.delete_access_key(UserName=user, AccessKeyId=key_id)
        try:
            resp = iam.list_access_keys(UserName=user)
            key_ids = [k["AccessKeyId"] for k in resp["AccessKeyMetadata"]]
            assert key_id not in key_ids
        finally:
            iam.delete_user(UserName=user)


class TestIAMRoleInlinePolicies:
    """Role inline policies: put, get, list, delete."""

    def test_put_role_policy(self, iam):
        role = _unique("role")
        pol = _unique("pol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.put_role_policy(
                RoleName=role, PolicyName=pol, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.list_role_policies(RoleName=role)
            assert pol in resp["PolicyNames"]
        finally:
            iam.delete_role_policy(RoleName=role, PolicyName=pol)
            iam.delete_role(RoleName=role)

    def test_get_role_policy(self, iam):
        role = _unique("role")
        pol = _unique("pol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.put_role_policy(
            RoleName=role, PolicyName=pol, PolicyDocument=SIMPLE_POLICY_DOC
        )
        try:
            resp = iam.get_role_policy(RoleName=role, PolicyName=pol)
            assert resp["RoleName"] == role
            assert resp["PolicyName"] == pol
            assert resp["PolicyDocument"] is not None
        finally:
            iam.delete_role_policy(RoleName=role, PolicyName=pol)
            iam.delete_role(RoleName=role)

    def test_list_role_policies(self, iam):
        role = _unique("role")
        pol1 = _unique("pol")
        pol2 = _unique("pol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.put_role_policy(
            RoleName=role, PolicyName=pol1, PolicyDocument=SIMPLE_POLICY_DOC
        )
        iam.put_role_policy(
            RoleName=role, PolicyName=pol2, PolicyDocument=SIMPLE_POLICY_DOC
        )
        try:
            resp = iam.list_role_policies(RoleName=role)
            assert pol1 in resp["PolicyNames"]
            assert pol2 in resp["PolicyNames"]
        finally:
            iam.delete_role_policy(RoleName=role, PolicyName=pol1)
            iam.delete_role_policy(RoleName=role, PolicyName=pol2)
            iam.delete_role(RoleName=role)

    def test_delete_role_policy(self, iam):
        role = _unique("role")
        pol = _unique("pol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.put_role_policy(
            RoleName=role, PolicyName=pol, PolicyDocument=SIMPLE_POLICY_DOC
        )
        try:
            iam.delete_role_policy(RoleName=role, PolicyName=pol)
            resp = iam.list_role_policies(RoleName=role)
            assert pol not in resp["PolicyNames"]
        finally:
            iam.delete_role(RoleName=role)


class TestIAMUserInlinePolicies:
    """User inline policies: put, get, list."""

    def test_put_user_policy(self, iam):
        user = _unique("usr")
        pol = _unique("pol")
        iam.create_user(UserName=user)
        try:
            iam.put_user_policy(
                UserName=user, PolicyName=pol, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.list_user_policies(UserName=user)
            assert pol in resp["PolicyNames"]
        finally:
            iam.delete_user_policy(UserName=user, PolicyName=pol)
            iam.delete_user(UserName=user)

    def test_get_user_policy(self, iam):
        user = _unique("usr")
        pol = _unique("pol")
        iam.create_user(UserName=user)
        iam.put_user_policy(
            UserName=user, PolicyName=pol, PolicyDocument=SIMPLE_POLICY_DOC
        )
        try:
            resp = iam.get_user_policy(UserName=user, PolicyName=pol)
            assert resp["UserName"] == user
            assert resp["PolicyName"] == pol
            assert resp["PolicyDocument"] is not None
        finally:
            iam.delete_user_policy(UserName=user, PolicyName=pol)
            iam.delete_user(UserName=user)

    def test_list_user_policies(self, iam):
        user = _unique("usr")
        pol1 = _unique("pol")
        pol2 = _unique("pol")
        iam.create_user(UserName=user)
        iam.put_user_policy(
            UserName=user, PolicyName=pol1, PolicyDocument=SIMPLE_POLICY_DOC
        )
        iam.put_user_policy(
            UserName=user, PolicyName=pol2, PolicyDocument=SIMPLE_POLICY_DOC
        )
        try:
            resp = iam.list_user_policies(UserName=user)
            assert pol1 in resp["PolicyNames"]
            assert pol2 in resp["PolicyNames"]
        finally:
            iam.delete_user_policy(UserName=user, PolicyName=pol1)
            iam.delete_user_policy(UserName=user, PolicyName=pol2)
            iam.delete_user(UserName=user)


class TestIAMTagRole:
    """Role tagging: tag, untag, list tags."""

    def test_tag_role(self, iam):
        role = _unique("role")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.tag_role(
                RoleName=role,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "core"}],
            )
            resp = iam.list_role_tags(RoleName=role)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "core"
        finally:
            iam.delete_role(RoleName=role)

    def test_untag_role(self, iam):
        role = _unique("role")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.tag_role(
            RoleName=role,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "core"}],
        )
        try:
            iam.untag_role(RoleName=role, TagKeys=["env"])
            resp = iam.list_role_tags(RoleName=role)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "env" not in tag_keys
            assert "team" in tag_keys
        finally:
            iam.delete_role(RoleName=role)

    def test_list_role_tags_empty(self, iam):
        role = _unique("role")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.list_role_tags(RoleName=role)
            assert resp["Tags"] == []
        finally:
            iam.delete_role(RoleName=role)


class TestIAMTagUser:
    """User tagging: tag, untag."""

    def test_tag_user(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        try:
            iam.tag_user(
                UserName=user,
                Tags=[{"Key": "department", "Value": "eng"}],
            )
            resp = iam.list_user_tags(UserName=user)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["department"] == "eng"
        finally:
            iam.delete_user(UserName=user)

    def test_untag_user(self, iam):
        user = _unique("usr")
        iam.create_user(UserName=user)
        iam.tag_user(
            UserName=user,
            Tags=[{"Key": "department", "Value": "eng"}, {"Key": "level", "Value": "5"}],
        )
        try:
            iam.untag_user(UserName=user, TagKeys=["department"])
            resp = iam.list_user_tags(UserName=user)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "department" not in tag_keys
            assert "level" in tag_keys
        finally:
            iam.delete_user(UserName=user)
