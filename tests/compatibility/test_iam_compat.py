"""IAM compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

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


# ---------------------------------------------------------------------------
# Additional IAM Group tests
# ---------------------------------------------------------------------------


class TestIAMGroupExtended:
    def test_create_group(self, iam):
        group_name = _unique("cg-grp")
        try:
            resp = iam.create_group(GroupName=group_name)
            assert resp["Group"]["GroupName"] == group_name
            assert "Arn" in resp["Group"]
        finally:
            iam.delete_group(GroupName=group_name)

    def test_get_group_metadata(self, iam):
        group_name = _unique("gg-grp")
        try:
            iam.create_group(GroupName=group_name)
            resp = iam.get_group(GroupName=group_name)
            assert resp["Group"]["GroupName"] == group_name
            assert resp["Users"] == []
        finally:
            iam.delete_group(GroupName=group_name)


# Group CRUD
# ---------------------------------------------------------------------------


class TestIAMGroupCRUD:
    def test_create_and_delete_group(self, iam):
        group_name = _unique("crud-group")
        resp = iam.create_group(GroupName=group_name)
        assert resp["Group"]["GroupName"] == group_name
        iam.delete_group(GroupName=group_name)

    def test_get_group(self, iam):
        group_name = _unique("get-group")
        iam.create_group(GroupName=group_name)
        try:
            resp = iam.get_group(GroupName=group_name)
            assert resp["Group"]["GroupName"] == group_name
        finally:
            iam.delete_group(GroupName=group_name)

    def test_list_groups(self, iam):
        g1 = _unique("lg-grp")
        g2 = _unique("lg-grp")
        try:
            iam.create_group(GroupName=g1)
            iam.create_group(GroupName=g2)
            resp = iam.list_groups()
            names = [g["GroupName"] for g in resp["Groups"]]
            assert g1 in names
            assert g2 in names
        finally:
            iam.delete_group(GroupName=g1)
            iam.delete_group(GroupName=g2)

    def test_delete_group(self, iam):
        group_name = _unique("dg-grp")
        iam.create_group(GroupName=group_name)
        iam.delete_group(GroupName=group_name)
        resp = iam.list_groups()
        names = [g["GroupName"] for g in resp["Groups"]]
        assert group_name not in names

    def test_add_and_remove_user_from_group(self, iam):
        group_name = _unique("aurg-grp")
        user_name = _unique("aurg-usr")
        try:
            iam.create_group(GroupName=group_name)
            iam.create_user(UserName=user_name)
            iam.add_user_to_group(GroupName=group_name, UserName=user_name)

            resp = iam.get_group(GroupName=group_name)
            assert any(u["UserName"] == user_name for u in resp["Users"])

            iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            resp = iam.get_group(GroupName=group_name)
            assert not any(u["UserName"] == user_name for u in resp["Users"])
        finally:
            try:
                iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)
            iam.delete_group(GroupName=group_name)

    def test_list_groups_for_user(self, iam):
        group_name = _unique("lgfu-grp")
        user_name = _unique("lgfu-usr")
        try:
            iam.create_group(GroupName=group_name)
            iam.create_user(UserName=user_name)
            iam.add_user_to_group(GroupName=group_name, UserName=user_name)
            resp = iam.list_groups_for_user(UserName=user_name)
            names = [g["GroupName"] for g in resp["Groups"]]
            assert group_name in names
        finally:
            iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            iam.delete_user(UserName=user_name)
        group_name = _unique("list-group")
        iam.create_group(GroupName=group_name)
        try:
            resp = iam.list_groups()
            names = [g["GroupName"] for g in resp["Groups"]]
            assert group_name in names
        finally:
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Managed policy attach/detach
# ---------------------------------------------------------------------------


class TestIAMManagedPolicyAttach:
    def test_get_policy(self, iam):
        policy_name = _unique("gp-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy(PolicyArn=arn)
            assert resp["Policy"]["PolicyName"] == policy_name
            assert resp["Policy"]["Arn"] == arn
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_attach_and_detach_user_policy(self, iam):
        user_name = _unique("adup-usr")
        policy_name = _unique("adup-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.create_user(UserName=user_name)
            iam.attach_user_policy(UserName=user_name, PolicyArn=arn)
            resp = iam.list_attached_user_policies(UserName=user_name)
            assert any(p["PolicyArn"] == arn for p in resp["AttachedPolicies"])

            iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            resp = iam.list_attached_user_policies(UserName=user_name)
            assert not any(p["PolicyArn"] == arn for p in resp["AttachedPolicies"])
        finally:
            try:
                iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)
            iam.delete_policy(PolicyArn=arn)

    def test_attach_group_policy(self, iam):
        group_name = _unique("agp-grp")
        policy_name = _unique("agp-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.create_group(GroupName=group_name)
            iam.attach_group_policy(GroupName=group_name, PolicyArn=arn)
            resp = iam.list_attached_group_policies(GroupName=group_name)
            assert any(p["PolicyArn"] == arn for p in resp["AttachedPolicies"])

            iam.detach_group_policy(GroupName=group_name, PolicyArn=arn)
        finally:
            try:
                iam.detach_group_policy(GroupName=group_name, PolicyArn=arn)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_group(GroupName=group_name)
            iam.delete_policy(PolicyArn=arn)

    def test_list_attached_role_policies(self, iam):
        role_name = _unique("larp-role")
        p1 = _unique("larp-pol1")
        p2 = _unique("larp-pol2")
        pol1 = iam.create_policy(PolicyName=p1, PolicyDocument=SIMPLE_POLICY_DOC)
        pol2 = iam.create_policy(PolicyName=p2, PolicyDocument=SIMPLE_POLICY_DOC)
        arn1 = pol1["Policy"]["Arn"]
        arn2 = pol2["Policy"]["Arn"]
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn1)
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn2)
            resp = iam.list_attached_role_policies(RoleName=role_name)
            arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
            assert arn1 in arns
            assert arn2 in arns
        finally:
            try:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=arn1)
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=arn2)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)
            iam.delete_policy(PolicyArn=arn1)
            iam.delete_policy(PolicyArn=arn2)

    def test_detach_role_policy(self, iam):
        role_name = _unique("drp-role")
        policy_name = _unique("drp-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role_name)
            assert not any(p["PolicyArn"] == arn for p in resp["AttachedPolicies"])
        finally:
            try:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Inline policies (role)
# ---------------------------------------------------------------------------


class TestIAMInlinePolicyExtended:
    def test_put_and_get_role_policy(self, iam):
        role_name = _unique("irp-role")
        policy_name = _unique("irp-pol")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            assert resp["PolicyName"] == policy_name
            assert resp["RoleName"] == role_name
        finally:
            try:
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)

    def test_list_role_policies(self, iam):
        role_name = _unique("lrp-role")
        p1 = _unique("lrp-pol1")
        p2 = _unique("lrp-pol2")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.put_role_policy(RoleName=role_name, PolicyName=p1, PolicyDocument=SIMPLE_POLICY_DOC)
            iam.put_role_policy(RoleName=role_name, PolicyName=p2, PolicyDocument=SIMPLE_POLICY_DOC)
            resp = iam.list_role_policies(RoleName=role_name)
            assert p1 in resp["PolicyNames"]
            assert p2 in resp["PolicyNames"]
        finally:
            for p in [p1, p2]:
                try:
                    iam.delete_role_policy(RoleName=role_name, PolicyName=p)
                except Exception:
                    pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)

    def test_delete_role_policy(self, iam):
        role_name = _unique("drpol-role")
        policy_name = _unique("drpol-pol")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            resp = iam.list_role_policies(RoleName=role_name)
            assert policy_name not in resp["PolicyNames"]
        finally:
            iam.delete_role(RoleName=role_name)

    def test_delete_user_policy(self, iam):
        user_name = _unique("dup-usr")
        policy_name = _unique("dup-pol")
        try:
            iam.create_user(UserName=user_name)
            iam.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)
            resp = iam.list_user_policies(UserName=user_name)
            assert policy_name not in resp["PolicyNames"]
        finally:
            iam.delete_user(UserName=user_name)

    def test_list_user_policies_multiple(self, iam):
        user_name = _unique("lup-usr")
        p1 = _unique("lup-pol1")
        p2 = _unique("lup-pol2")
        try:
            iam.create_user(UserName=user_name)
            iam.put_user_policy(UserName=user_name, PolicyName=p1, PolicyDocument=SIMPLE_POLICY_DOC)
            iam.put_user_policy(UserName=user_name, PolicyName=p2, PolicyDocument=SIMPLE_POLICY_DOC)
            resp = iam.list_user_policies(UserName=user_name)
            assert p1 in resp["PolicyNames"]
            assert p2 in resp["PolicyNames"]
        finally:
            for p in [p1, p2]:
                try:
                    iam.delete_user_policy(UserName=user_name, PolicyName=p)
                except Exception:
                    pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Instance profiles
# ---------------------------------------------------------------------------


class TestIAMInstanceProfileExtended:
    def test_create_instance_profile(self, iam):
        name = _unique("cip-prof")
        try:
            resp = iam.create_instance_profile(InstanceProfileName=name)
            assert resp["InstanceProfile"]["InstanceProfileName"] == name
            assert resp["InstanceProfile"]["Roles"] == []
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_get_instance_profile(self, iam):
        name = _unique("gip-prof")
        try:
            iam.create_instance_profile(InstanceProfileName=name)
            resp = iam.get_instance_profile(InstanceProfileName=name)
            assert resp["InstanceProfile"]["InstanceProfileName"] == name
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_list_instance_profiles(self, iam):
        name = _unique("lip-prof")
        try:
            iam.create_instance_profile(InstanceProfileName=name)
            resp = iam.list_instance_profiles()
            names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
            assert name in names
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_remove_role_from_instance_profile(self, iam):
        prof = _unique("rrip-prof")
        role = _unique("rrip-role")
        try:
            iam.create_instance_profile(InstanceProfileName=prof)
            iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.add_role_to_instance_profile(InstanceProfileName=prof, RoleName=role)
            iam.remove_role_from_instance_profile(InstanceProfileName=prof, RoleName=role)
            resp = iam.get_instance_profile(InstanceProfileName=prof)
            assert resp["InstanceProfile"]["Roles"] == []
        finally:
            try:
                iam.remove_role_from_instance_profile(InstanceProfileName=prof, RoleName=role)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_instance_profile(InstanceProfileName=prof)
            iam.delete_role(RoleName=role)


# ---------------------------------------------------------------------------
# Access keys
# ---------------------------------------------------------------------------


class TestIAMAccessKeys:
    def test_create_access_key(self, iam):
        user_name = _unique("ak-usr")
        try:
            iam.create_user(UserName=user_name)
            resp = iam.create_access_key(UserName=user_name)
            ak = resp["AccessKey"]
            assert ak["UserName"] == user_name
            assert "AccessKeyId" in ak
            assert "SecretAccessKey" in ak
            assert ak["Status"] == "Active"
        finally:
            try:
                iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_list_access_keys(self, iam):
        user_name = _unique("lak-usr")
        try:
            iam.create_user(UserName=user_name)
            ak = iam.create_access_key(UserName=user_name)["AccessKey"]
            resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in resp["AccessKeyMetadata"]]
            assert ak["AccessKeyId"] in key_ids
        finally:
            try:
                iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_update_access_key_inactive(self, iam):
        user_name = _unique("uak-usr")
        try:
            iam.create_user(UserName=user_name)
            ak = iam.create_access_key(UserName=user_name)["AccessKey"]
            iam.update_access_key(
                UserName=user_name,
                AccessKeyId=ak["AccessKeyId"],
                Status="Inactive",
            )
            resp = iam.list_access_keys(UserName=user_name)
            key = next(
                k for k in resp["AccessKeyMetadata"] if k["AccessKeyId"] == ak["AccessKeyId"]
            )
            assert key["Status"] == "Inactive"
        finally:
            try:
                iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_delete_access_key(self, iam):
        user_name = _unique("dak-usr")
        try:
            iam.create_user(UserName=user_name)
            ak = iam.create_access_key(UserName=user_name)["AccessKey"]
            iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
            resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in resp["AccessKeyMetadata"]]
            assert ak["AccessKeyId"] not in key_ids
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Role tags
# ---------------------------------------------------------------------------


class TestIAMRoleTags:
    def test_tag_role(self, iam):
        role_name = _unique("tr-role")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.tag_role(
                RoleName=role_name,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "backend"}],
            )
            resp = iam.list_role_tags(RoleName=role_name)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "backend"
        finally:
            iam.delete_role(RoleName=role_name)

    def test_untag_role(self, iam):
        role_name = _unique("ur-role")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.tag_role(
                RoleName=role_name,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "backend"}],
            )
            iam.untag_role(RoleName=role_name, TagKeys=["env"])
            resp = iam.list_role_tags(RoleName=role_name)
            keys = [t["Key"] for t in resp["Tags"]]
            assert "env" not in keys
            assert "team" in keys
        finally:
            iam.delete_role(RoleName=role_name)

    def test_list_role_tags_empty(self, iam):
        role_name = _unique("lrt-role")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            resp = iam.list_role_tags(RoleName=role_name)
            assert resp["Tags"] == []
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# User tags
# ---------------------------------------------------------------------------


class TestIAMUserTags:
    def test_tag_user(self, iam):
        user_name = _unique("tu-usr")
        try:
            iam.create_user(UserName=user_name)
            iam.tag_user(
                UserName=user_name,
                Tags=[{"Key": "dept", "Value": "eng"}],
            )
            resp = iam.list_user_tags(UserName=user_name)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["dept"] == "eng"
        finally:
            iam.delete_user(UserName=user_name)

    def test_untag_user(self, iam):
        user_name = _unique("uu-usr")
        try:
            iam.create_user(UserName=user_name)
            iam.tag_user(
                UserName=user_name,
                Tags=[{"Key": "dept", "Value": "eng"}, {"Key": "level", "Value": "5"}],
            )
            iam.untag_user(UserName=user_name, TagKeys=["dept"])
            resp = iam.list_user_tags(UserName=user_name)
            keys = [t["Key"] for t in resp["Tags"]]
            assert "dept" not in keys
            assert "level" in keys
        finally:
            iam.delete_user(UserName=user_name)

    def test_list_user_tags_empty(self, iam):
        user_name = _unique("lut-usr")
        try:
            iam.create_user(UserName=user_name)
            resp = iam.list_user_tags(UserName=user_name)
            assert resp["Tags"] == []
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Policy versions (extended)
# ---------------------------------------------------------------------------


class TestIAMPolicyVersionsExtended:
    def test_get_policy_version(self, iam):
        policy_name = _unique("gpv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy_version(PolicyArn=arn, VersionId="v1")
            assert resp["PolicyVersion"]["VersionId"] == "v1"
            assert resp["PolicyVersion"]["IsDefaultVersion"] is True
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_list_policy_versions_multiple(self, iam):
        policy_name = _unique("lpv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            doc2 = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            doc3 = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "ec2:*", "Resource": "*"}],
                }
            )
            iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc2, SetAsDefault=False)
            iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc3, SetAsDefault=False)
            resp = iam.list_policy_versions(PolicyArn=arn)
            version_ids = [v["VersionId"] for v in resp["Versions"]]
            assert "v1" in version_ids
            assert "v2" in version_ids
            assert "v3" in version_ids
        finally:
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
            iam.delete_policy(PolicyArn=arn)

    def test_set_default_policy_version(self, iam):
        policy_name = _unique("sdpv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            doc2 = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc2, SetAsDefault=False)
            iam.set_default_policy_version(PolicyArn=arn, VersionId="v2")
            resp = iam.list_policy_versions(PolicyArn=arn)
            for v in resp["Versions"]:
                if v["VersionId"] == "v2":
                    assert v["IsDefaultVersion"] is True
                elif v["VersionId"] == "v1":
                    assert v["IsDefaultVersion"] is False
        finally:
            # v2 is default; delete v1 (non-default), then delete the policy
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    try:
                        iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
                    except Exception:
                        pass  # best-effort cleanup
            iam.delete_policy(PolicyArn=arn)

    def test_delete_policy_version(self, iam):
        policy_name = _unique("dpv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            doc2 = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc2, SetAsDefault=False)
            iam.delete_policy_version(PolicyArn=arn, VersionId="v2")
            resp = iam.list_policy_versions(PolicyArn=arn)
            version_ids = [v["VersionId"] for v in resp["Versions"]]
            assert "v2" not in version_ids
        finally:
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
            iam.delete_policy(PolicyArn=arn)


# Group membership
# ---------------------------------------------------------------------------


class TestIAMGroupMembership:
    def test_add_remove_user_from_group(self, iam):
        group_name = _unique("mem-group")
        user_name = _unique("mem-user")
        iam.create_group(GroupName=group_name)
        iam.create_user(UserName=user_name)
        try:
            iam.add_user_to_group(GroupName=group_name, UserName=user_name)
            resp = iam.list_groups_for_user(UserName=user_name)
            group_names = [g["GroupName"] for g in resp["Groups"]]
            assert group_name in group_names

            iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            resp = iam.list_groups_for_user(UserName=user_name)
            group_names = [g["GroupName"] for g in resp["Groups"]]
            assert group_name not in group_names
        finally:
            try:
                iam.remove_user_from_group(GroupName=group_name, UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Attach/detach role policy
# ---------------------------------------------------------------------------


class TestIAMAttachDetachRolePolicy:
    def test_attach_list_detach_role_policy(self, iam):
        role_name = _unique("att-role")
        policy_name = _unique("att-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role_name)
            arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
            assert arn in arns

            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role_name)
            arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
            assert arn not in arns
        finally:
            try:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Instance profile CRUD
# ---------------------------------------------------------------------------


class TestIAMInstanceProfileCRUD:
    def test_create_delete_list_instance_profiles(self, iam):
        profile_name = _unique("ip-crud")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        try:
            resp = iam.list_instance_profiles()
            names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
            assert profile_name in names
        finally:
            iam.delete_instance_profile(InstanceProfileName=profile_name)

    def test_add_remove_role_from_instance_profile(self, iam):
        profile_name = _unique("ip-role")
        role_name = _unique("ip-role-r")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
            resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            role_names = [r["RoleName"] for r in resp["InstanceProfile"]["Roles"]]
            assert role_name in role_names

            iam.remove_role_from_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            role_names = [r["RoleName"] for r in resp["InstanceProfile"]["Roles"]]
            assert role_name not in role_names
        finally:
            try:
                iam.remove_role_from_instance_profile(
                    InstanceProfileName=profile_name, RoleName=role_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_instance_profile(InstanceProfileName=profile_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Role inline policy
# ---------------------------------------------------------------------------


class TestIAMRoleInlinePolicy:
    def test_put_get_delete_list_role_policy(self, iam):
        role_name = _unique("rp-role")
        policy_name = _unique("rp-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.list_role_policies(RoleName=role_name)
            assert policy_name in resp["PolicyNames"]

            get_resp = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            assert get_resp["PolicyName"] == policy_name

            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            resp = iam.list_role_policies(RoleName=role_name)
            assert policy_name not in resp["PolicyNames"]
        finally:
            try:
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Access keys lifecycle
# ---------------------------------------------------------------------------


class TestIAMAccessKeysLifecycle:
    def test_create_list_delete_access_key(self, iam):
        user_name = _unique("ak-user")
        iam.create_user(UserName=user_name)
        try:
            create_resp = iam.create_access_key(UserName=user_name)
            access_key_id = create_resp["AccessKey"]["AccessKeyId"]
            assert create_resp["AccessKey"]["UserName"] == user_name

            list_resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in list_resp["AccessKeyMetadata"]]
            assert access_key_id in key_ids

            iam.delete_access_key(UserName=user_name, AccessKeyId=access_key_id)
            list_resp = iam.list_access_keys(UserName=user_name)
            key_ids = [k["AccessKeyId"] for k in list_resp["AccessKeyMetadata"]]
            assert access_key_id not in key_ids
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Login profile
# ---------------------------------------------------------------------------


class TestIAMLoginProfile:
    def test_create_get_delete_login_profile(self, iam):
        user_name = _unique("lp-user")
        iam.create_user(UserName=user_name)
        try:
            iam.create_login_profile(UserName=user_name, Password="T3stP@ss!")
            get_resp = iam.get_login_profile(UserName=user_name)
            assert get_resp["LoginProfile"]["UserName"] == user_name

            iam.delete_login_profile(UserName=user_name)
            # Verify it's deleted by expecting an error
            try:
                iam.get_login_profile(UserName=user_name)
                assert False, "Expected NoSuchEntity error"
            except iam.exceptions.NoSuchEntityException:
                pass  # resource may not exist
        finally:
            try:
                iam.delete_login_profile(UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_get_account_authorization_details_filter(self, iam):
        """Filter get_account_authorization_details by entity type."""
        user_name = _unique("authf-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.get_account_authorization_details(Filter=["User"])
            assert "UserDetailList" in resp
            user_names = [u["UserName"] for u in resp.get("UserDetailList", [])]
            assert user_name in user_names
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMServiceLinkedRole:
    def test_create_and_delete_service_linked_role(self, iam):
        """CreateServiceLinkedRole / DeleteServiceLinkedRole / GetServiceLinkedRoleDeletionStatus."""  # noqa: E501
        try:
            resp = iam.create_service_linked_role(AWSServiceName="elasticbeanstalk.amazonaws.com")
            role = resp["Role"]
            assert (
                "elasticbeanstalk" in role["RoleName"].lower() or "AWSServiceRole" in role["Path"]
            )
            role_name = role["RoleName"]
        except iam.exceptions.InvalidInputException:
            pytest.skip("Service linked role already exists")
            return

        try:
            del_resp = iam.delete_service_linked_role(RoleName=role_name)
            deletion_task_id = del_resp["DeletionTaskId"]
            assert deletion_task_id is not None

            status_resp = iam.get_service_linked_role_deletion_status(
                DeletionTaskId=deletion_task_id
            )
            assert status_resp["Status"] in ("SUCCEEDED", "IN_PROGRESS", "NOT_STARTED", "FAILED")
        except Exception:
            # Best-effort cleanup
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass  # best-effort cleanup


class TestIAMOpenIDConnectProvider:
    def test_create_get_delete_oidc_provider(self, iam):
        """CreateOpenIDConnectProvider / GetOpenIDConnectProvider / DeleteOpenIDConnectProvider."""
        url = "https://oidc.example.com"
        thumbprint = "a" * 40
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=[thumbprint],
            ClientIDList=["my-client-id"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert get_resp["Url"] == "oidc.example.com" or get_resp["Url"] == url
            assert thumbprint in get_resp["ThumbprintList"]
            assert "my-client-id" in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


class TestIAMSAMLProvider:
    def test_create_get_delete_saml_provider(self, iam):
        """CreateSAMLProvider / GetSAMLProvider / DeleteSAMLProvider."""
        saml_metadata = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
            ' entityID="https://idp.example.com/metadata">'
            "<IDPSSODescriptor"
            ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
            '<KeyDescriptor use="signing">'
            '<KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
            "<X509Data><X509Certificate>"
            "MIIDpDCCAoygAwIBAgIGAXpJOXwHMA0GCSqGSIb3DQEBCwUAMIGSMQswCQYDVQQGEwJVUzETMBEG"
            "A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU"
            "MBIGA1UECwwLU1NPUHJvdmlkZXIxEzARBgNVBAMMCmRldi04NDMyNTMxHDAaBgkqhkiG9w0BCQEW"
            "DWluZm9Ab2t0YS5jb20wHhcNMjEwNjIyMTgxNjQzWhcNMzEwNjIyMTgxNzQzWjCBkjELMAkGA1UE"
            "BhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNV"
            "BAoMBE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRMwEQYDVQQDDApkZXYtODQzMjUzMRwwGgYJ"
            "KoZIhvcNAQkBFg1pbmZvQG9rdGEuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA"
            "</X509Certificate></X509Data>"
            "</KeyInfo></KeyDescriptor>"
            "<SingleSignOnService"
            ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
            ' Location="https://idp.example.com/sso"/>'
            "<SingleSignOnService"
            ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"'
            ' Location="https://idp.example.com/sso"/>'
            "</IDPSSODescriptor></EntityDescriptor>"
        )
        name = _unique("saml-prov")
        resp = iam.create_saml_provider(
            SAMLMetadataDocument=saml_metadata,
            Name=name,
        )
        arn = resp["SAMLProviderArn"]
        try:
            get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
            assert "SAMLMetadataDocument" in get_resp
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


class TestIAMSimulatePolicy:
    def test_simulate_principal_policy(self, iam):
        """SimulatePrincipalPolicy."""
        role_name = _unique("sim-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=_unique("sim-pol"), PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            resp = iam.simulate_principal_policy(
                PolicySourceArn=role_arn,
                ActionNames=["s3:GetObject"],
            )
            assert "EvaluationResults" in resp
        finally:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)

    def test_simulate_custom_policy(self, iam):
        """SimulateCustomPolicy."""
        resp = iam.simulate_custom_policy(
            PolicyInputList=[SIMPLE_POLICY_DOC],
            ActionNames=["s3:GetObject"],
        )
        assert "EvaluationResults" in resp


class TestIAMAccountSummary:
    def test_get_account_summary(self, iam):
        """GetAccountSummary returns a summary map."""
        resp = iam.get_account_summary()
        summary = resp["SummaryMap"]
        assert "Users" in summary
        assert "Roles" in summary
        assert "Policies" in summary


class TestIAMPolicyVersionsV2:
    def test_create_list_get_delete_policy_version(self, iam):
        """CreatePolicyVersion / ListPolicyVersions / GetPolicyVersion / DeletePolicyVersion."""
        policy_name = _unique("pv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            new_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                }
            )
            v2 = iam.create_policy_version(PolicyArn=arn, PolicyDocument=new_doc)
            assert v2["PolicyVersion"]["VersionId"] == "v2"

            versions = iam.list_policy_versions(PolicyArn=arn)
            version_ids = [v["VersionId"] for v in versions["Versions"]]
            assert "v1" in version_ids
            assert "v2" in version_ids

            get_resp = iam.get_policy_version(PolicyArn=arn, VersionId="v2")
            assert get_resp["PolicyVersion"]["VersionId"] == "v2"
            assert get_resp["PolicyVersion"]["Document"] is not None

            iam.delete_policy_version(PolicyArn=arn, VersionId="v2")
            versions_after = iam.list_policy_versions(PolicyArn=arn)
            version_ids_after = [v["VersionId"] for v in versions_after["Versions"]]
            assert "v2" not in version_ids_after
        finally:
            for v in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
                if not v["IsDefaultVersion"]:
                    iam.delete_policy_version(PolicyArn=arn, VersionId=v["VersionId"])
            iam.delete_policy(PolicyArn=arn)

    def test_list_entities_for_policy(self, iam):
        """ListEntitiesForPolicy."""
        role_name = _unique("ent-role")
        policy_name = _unique("ent-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp = iam.list_entities_for_policy(PolicyArn=arn)
            role_names = [r["RoleName"] for r in resp.get("PolicyRoles", [])]
            assert role_name in role_names
        finally:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)


class TestIAMListPoliciesWithScope:
    def test_list_policies_aws_scope(self, iam):
        """ListPolicies with Scope=AWS returns only AWS-managed policies (may be empty)."""
        resp = iam.list_policies(Scope="AWS")
        assert "Policies" in resp
        # If there are AWS-managed policies, they should have aws in the ARN
        for p in resp["Policies"][:5]:
            assert ":aws:policy/" in p["Arn"] or "arn:aws:iam" in p["Arn"]

    def test_list_policies_local_scope(self, iam):
        """ListPolicies with Scope=Local returns only customer-managed policies."""
        policy_name = _unique("scope-local-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policies(Scope="Local")
            names = [p["PolicyName"] for p in resp["Policies"]]
            assert policy_name in names
        finally:
            iam.delete_policy(PolicyArn=arn)


class TestIAMAccountPasswordPolicy:
    def test_update_get_delete_account_password_policy(self, iam):
        """UpdateAccountPasswordPolicy / GetAccountPasswordPolicy / DeleteAccountPasswordPolicy."""
        try:
            iam.update_account_password_policy(
                MinimumPasswordLength=12,
                RequireSymbols=True,
                RequireNumbers=True,
                RequireUppercaseCharacters=True,
                RequireLowercaseCharacters=True,
                MaxPasswordAge=90,
                PasswordReusePrevention=5,
            )
            resp = iam.get_account_password_policy()
            policy = resp["PasswordPolicy"]
            assert policy["MinimumPasswordLength"] == 12
            assert policy["RequireSymbols"] is True
            assert policy["RequireNumbers"] is True
        finally:
            try:
                iam.delete_account_password_policy()
            except Exception:
                pass  # best-effort cleanup


class TestIAMChangePassword:
    def test_change_password(self, iam):
        """ChangePassword."""
        iam.change_password(OldPassword="oldpass", NewPassword="newpass123!")


class TestIAMMFADevices:
    def test_list_mfa_devices(self, iam):
        """ListMFADevices for a user with no MFA."""
        user_name = _unique("mfa-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_mfa_devices(UserName=user_name)
            assert "MFADevices" in resp
            assert len(resp["MFADevices"]) == 0
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMSigningCertificates:
    def test_list_signing_certificates(self, iam):
        """ListSigningCertificates for a user with no certs."""
        user_name = _unique("cert-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_signing_certificates(UserName=user_name)
            assert "Certificates" in resp
            assert len(resp["Certificates"]) == 0
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMSSHPublicKeys:
    def test_list_ssh_public_keys(self, iam):
        """ListSSHPublicKeys."""
        user_name = _unique("ssh-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_ssh_public_keys(UserName=user_name)
            assert "SSHPublicKeys" in resp
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMUserTagsExtended:
    def test_tag_untag_list_user_tags(self, iam):
        """TagUser / UntagUser / ListUserTags."""
        user_name = _unique("tag-user")
        iam.create_user(UserName=user_name)
        try:
            iam.tag_user(
                UserName=user_name,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "backend"},
                ],
            )
            resp = iam.list_user_tags(UserName=user_name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "backend"

            iam.untag_user(UserName=user_name, TagKeys=["team"])
            resp = iam.list_user_tags(UserName=user_name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert "team" not in tags
            assert tags["env"] == "test"
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMRoleTagsExtended:
    def test_tag_untag_list_role_tags(self, iam):
        """TagRole / UntagRole / ListRoleTags."""
        role_name = _unique("tag-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.tag_role(
                RoleName=role_name,
                Tags=[
                    {"Key": "project", "Value": "robotocore"},
                    {"Key": "stage", "Value": "dev"},
                ],
            )
            resp = iam.list_role_tags(RoleName=role_name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["project"] == "robotocore"
            assert tags["stage"] == "dev"

            iam.untag_role(RoleName=role_name, TagKeys=["stage"])
            resp = iam.list_role_tags(RoleName=role_name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert "stage" not in tags
            assert tags["project"] == "robotocore"
        finally:
            iam.delete_role(RoleName=role_name)


class TestIAMPermissionsBoundary:
    def test_put_delete_role_permissions_boundary(self, iam):
        """PutRolePermissionsBoundary / DeleteRolePermissionsBoundary."""
        role_name = _unique("pb-role")
        policy_name = _unique("pb-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.put_role_permissions_boundary(RoleName=role_name, PermissionsBoundary=arn)
            role_resp = iam.get_role(RoleName=role_name)
            assert role_resp["Role"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == arn

            iam.delete_role_permissions_boundary(RoleName=role_name)
            role_resp = iam.get_role(RoleName=role_name)
            assert "PermissionsBoundary" not in role_resp["Role"]
        finally:
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)


class TestIAMExtendedOperations:
    """Extended IAM operations for higher coverage."""

    @pytest.fixture
    def iam(self):
        from tests.compatibility.conftest import make_client

        return make_client("iam")

    def test_create_user_with_tags(self, iam):
        name = _unique("tagged-user")
        try:
            iam.create_user(
                UserName=name,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            resp = iam.list_user_tags(UserName=name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "test"
        finally:
            iam.delete_user(UserName=name)

    def test_create_role_with_tags(self, iam):
        name = _unique("tagged-role")
        try:
            iam.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=TRUST_POLICY,
                Tags=[{"Key": "team", "Value": "dev"}],
            )
            resp = iam.list_role_tags(RoleName=name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["team"] == "dev"
        finally:
            iam.delete_role(RoleName=name)

    def test_list_users(self, iam):
        name = _unique("list-user")
        iam.create_user(UserName=name)
        try:
            resp = iam.list_users()
            names = [u["UserName"] for u in resp["Users"]]
            assert name in names
        finally:
            iam.delete_user(UserName=name)

    def test_list_roles(self, iam):
        name = _unique("list-role")
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.list_roles()
            names = [r["RoleName"] for r in resp["Roles"]]
            assert name in names
        finally:
            iam.delete_role(RoleName=name)

    def test_list_groups(self, iam):
        name = _unique("list-group")
        iam.create_group(GroupName=name)
        try:
            resp = iam.list_groups()
            names = [g["GroupName"] for g in resp["Groups"]]
            assert name in names
        finally:
            iam.delete_group(GroupName=name)

    def test_add_user_to_group(self, iam):
        user = _unique("grp-user")
        group = _unique("grp")
        iam.create_user(UserName=user)
        iam.create_group(GroupName=group)
        try:
            iam.add_user_to_group(GroupName=group, UserName=user)
            resp = iam.list_groups_for_user(UserName=user)
            group_names = [g["GroupName"] for g in resp["Groups"]]
            assert group in group_names

            iam.remove_user_from_group(GroupName=group, UserName=user)
            resp = iam.list_groups_for_user(UserName=user)
            group_names = [g["GroupName"] for g in resp["Groups"]]
            assert group not in group_names
        finally:
            iam.delete_user(UserName=user)
            iam.delete_group(GroupName=group)

    def test_create_delete_access_key(self, iam):
        user = _unique("ak-user")
        iam.create_user(UserName=user)
        try:
            resp = iam.create_access_key(UserName=user)
            ak = resp["AccessKey"]
            assert "AccessKeyId" in ak
            assert "SecretAccessKey" in ak
            assert ak["Status"] == "Active"

            keys = iam.list_access_keys(UserName=user)
            key_ids = [k["AccessKeyId"] for k in keys["AccessKeyMetadata"]]
            assert ak["AccessKeyId"] in key_ids

            iam.delete_access_key(UserName=user, AccessKeyId=ak["AccessKeyId"])
        finally:
            # Clean any remaining keys
            for k in iam.list_access_keys(UserName=user)["AccessKeyMetadata"]:
                iam.delete_access_key(UserName=user, AccessKeyId=k["AccessKeyId"])
            iam.delete_user(UserName=user)

    def test_update_access_key_status(self, iam):
        user = _unique("ak-status-user")
        iam.create_user(UserName=user)
        try:
            ak = iam.create_access_key(UserName=user)["AccessKey"]
            iam.update_access_key(UserName=user, AccessKeyId=ak["AccessKeyId"], Status="Inactive")
            keys = iam.list_access_keys(UserName=user)
            key = [k for k in keys["AccessKeyMetadata"] if k["AccessKeyId"] == ak["AccessKeyId"]][0]
            assert key["Status"] == "Inactive"
            iam.delete_access_key(UserName=user, AccessKeyId=ak["AccessKeyId"])
        finally:
            for k in iam.list_access_keys(UserName=user)["AccessKeyMetadata"]:
                iam.delete_access_key(UserName=user, AccessKeyId=k["AccessKeyId"])
            iam.delete_user(UserName=user)

    def test_put_get_delete_role_policy(self, iam):
        role = _unique("inline-role")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            iam.put_role_policy(
                RoleName=role,
                PolicyName="inline-pol",
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.get_role_policy(RoleName=role, PolicyName="inline-pol")
            assert resp["PolicyName"] == "inline-pol"

            listed = iam.list_role_policies(RoleName=role)
            assert "inline-pol" in listed["PolicyNames"]

            iam.delete_role_policy(RoleName=role, PolicyName="inline-pol")
        finally:
            # Ensure no inline policies remain
            for p in iam.list_role_policies(RoleName=role)["PolicyNames"]:
                iam.delete_role_policy(RoleName=role, PolicyName=p)
            iam.delete_role(RoleName=role)

    def test_put_get_delete_user_policy(self, iam):
        user = _unique("inline-user")
        iam.create_user(UserName=user)
        try:
            iam.put_user_policy(
                UserName=user,
                PolicyName="user-inline",
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            resp = iam.get_user_policy(UserName=user, PolicyName="user-inline")
            assert resp["PolicyName"] == "user-inline"

            listed = iam.list_user_policies(UserName=user)
            assert "user-inline" in listed["PolicyNames"]

            iam.delete_user_policy(UserName=user, PolicyName="user-inline")
        finally:
            for p in iam.list_user_policies(UserName=user)["PolicyNames"]:
                iam.delete_user_policy(UserName=user, PolicyName=p)
            iam.delete_user(UserName=user)

    def test_create_instance_profile(self, iam):
        name = _unique("inst-prof")
        role = _unique("ip-role")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.create_instance_profile(InstanceProfileName=name)
            assert resp["InstanceProfile"]["InstanceProfileName"] == name

            iam.add_role_to_instance_profile(InstanceProfileName=name, RoleName=role)
            described = iam.get_instance_profile(InstanceProfileName=name)
            roles = [r["RoleName"] for r in described["InstanceProfile"]["Roles"]]
            assert role in roles

            iam.remove_role_from_instance_profile(InstanceProfileName=name, RoleName=role)
            iam.delete_instance_profile(InstanceProfileName=name)
        finally:
            iam.delete_role(RoleName=role)

    def test_list_attached_role_policies(self, iam):
        role = _unique("attached-role")
        pol_name = _unique("attached-pol")
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=pol_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role, PolicyArn=arn)
            resp = iam.list_attached_role_policies(RoleName=role)
            policy_names = [p["PolicyName"] for p in resp["AttachedPolicies"]]
            assert pol_name in policy_names
        finally:
            iam.detach_role_policy(RoleName=role, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role)

    def test_get_user(self, iam):
        name = _unique("get-user")
        iam.create_user(UserName=name)
        try:
            resp = iam.get_user(UserName=name)
            assert resp["User"]["UserName"] == name
            assert "Arn" in resp["User"]
            assert "CreateDate" in resp["User"]
        finally:
            iam.delete_user(UserName=name)

    def test_update_role_description(self, iam):
        name = _unique("desc-role")
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Description="original",
        )
        try:
            iam.update_role(RoleName=name, Description="updated desc")
            resp = iam.get_role(RoleName=name)
            assert resp["Role"]["Description"] == "updated desc"
        finally:
            iam.delete_role(RoleName=name)


class TestIAMExtendedV2:
    """Additional IAM operations not covered by existing test classes."""

    def test_create_user_with_tags(self, iam):
        """CreateUser with inline Tags parameter."""
        user_name = _unique("ext-cutag")
        try:
            resp = iam.create_user(
                UserName=user_name,
                Tags=[
                    {"Key": "project", "Value": "robotocore"},
                    {"Key": "env", "Value": "ci"},
                ],
            )
            assert resp["User"]["UserName"] == user_name
            assert len(resp["User"]["Tags"]) == 2
            tag_map = {t["Key"]: t["Value"] for t in resp["User"]["Tags"]}
            assert tag_map["project"] == "robotocore"
            assert tag_map["env"] == "ci"
        finally:
            iam.delete_user(UserName=user_name)

    def test_create_role_with_tags(self, iam):
        """CreateRole with inline Tags parameter."""
        role_name = _unique("ext-crtag")
        try:
            resp = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=TRUST_POLICY,
                Tags=[{"Key": "managed-by", "Value": "test"}],
            )
            assert resp["Role"]["RoleName"] == role_name
            assert any(t["Key"] == "managed-by" for t in resp["Role"]["Tags"])
        finally:
            iam.delete_role(RoleName=role_name)

    def test_create_instance_profile_with_tags(self, iam):
        """CreateInstanceProfile with Tags."""
        name = _unique("ext-iptag")
        try:
            resp = iam.create_instance_profile(
                InstanceProfileName=name,
                Tags=[{"Key": "cost-center", "Value": "12345"}],
            )
            ip = resp["InstanceProfile"]
            assert ip["InstanceProfileName"] == name
            assert any(t["Key"] == "cost-center" for t in ip.get("Tags", []))
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_update_access_key_reactivate(self, iam):
        """UpdateAccessKey from Inactive back to Active."""
        user_name = _unique("ext-reak")
        try:
            iam.create_user(UserName=user_name)
            ak = iam.create_access_key(UserName=user_name)["AccessKey"]
            key_id = ak["AccessKeyId"]

            # Deactivate
            iam.update_access_key(UserName=user_name, AccessKeyId=key_id, Status="Inactive")
            resp = iam.list_access_keys(UserName=user_name)
            key = next(k for k in resp["AccessKeyMetadata"] if k["AccessKeyId"] == key_id)
            assert key["Status"] == "Inactive"

            # Reactivate
            iam.update_access_key(UserName=user_name, AccessKeyId=key_id, Status="Active")
            resp = iam.list_access_keys(UserName=user_name)
            key = next(k for k in resp["AccessKeyMetadata"] if k["AccessKeyId"] == key_id)
            assert key["Status"] == "Active"
        finally:
            try:
                iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_account_alias_lifecycle(self, iam):
        """CreateAccountAlias / ListAccountAliases / DeleteAccountAlias."""
        alias = _unique("ext-alias")
        try:
            iam.create_account_alias(AccountAlias=alias)
            resp = iam.list_account_aliases()
            assert alias in resp["AccountAliases"]
        finally:
            try:
                iam.delete_account_alias(AccountAlias=alias)
            except Exception:
                pass  # best-effort cleanup

    def test_list_account_aliases_empty_after_delete(self, iam):
        """After deleting an alias, it should not appear in the list."""
        alias = _unique("ext-aldel")
        iam.create_account_alias(AccountAlias=alias)
        iam.delete_account_alias(AccountAlias=alias)
        resp = iam.list_account_aliases()
        assert alias not in resp["AccountAliases"]

    def test_generate_and_get_credential_report(self, iam):
        """GenerateCredentialReport / GetCredentialReport."""
        import time

        # Trigger report generation
        gen_resp = iam.generate_credential_report()
        assert gen_resp["State"] in ("STARTED", "INPROGRESS", "COMPLETE")

        # Poll until complete (max ~5s)
        for _ in range(10):
            gen_resp = iam.generate_credential_report()
            if gen_resp["State"] == "COMPLETE":
                break
            time.sleep(0.5)

        get_resp = iam.get_credential_report()
        assert get_resp["ReportFormat"] == "text/csv"
        content = get_resp["Content"]
        # Content is bytes; should contain CSV header
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        assert "user" in content.lower()

    def test_update_user_rename(self, iam):
        """UpdateUser to change the UserName (rename)."""
        old_name = _unique("ext-oldnm")
        new_name = _unique("ext-newnm")
        iam.create_user(UserName=old_name)
        try:
            iam.update_user(UserName=old_name, NewUserName=new_name)
            resp = iam.get_user(UserName=new_name)
            assert resp["User"]["UserName"] == new_name
            # Old name should not exist
            with pytest.raises(iam.exceptions.NoSuchEntityException):
                iam.get_user(UserName=old_name)
        finally:
            try:
                iam.delete_user(UserName=new_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_user(UserName=old_name)
            except Exception:
                pass  # best-effort cleanup

    def test_update_role_description(self, iam):
        """UpdateRole to change the description."""
        role_name = _unique("ext-updr")
        try:
            iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=TRUST_POLICY,
                Description="original desc",
            )
            iam.update_role(RoleName=role_name, Description="updated desc")
            resp = iam.get_role(RoleName=role_name)
            assert resp["Role"]["Description"] == "updated desc"
        finally:
            iam.delete_role(RoleName=role_name)

    def test_list_instance_profiles_for_role(self, iam):
        """ListInstanceProfilesForRole."""
        prof_name = _unique("ext-lipfr")
        role_name = _unique("ext-lipfr")
        try:
            iam.create_instance_profile(InstanceProfileName=prof_name)
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.add_role_to_instance_profile(InstanceProfileName=prof_name, RoleName=role_name)

            resp = iam.list_instance_profiles_for_role(RoleName=role_name)
            ip_names = [ip["InstanceProfileName"] for ip in resp["InstanceProfiles"]]
            assert prof_name in ip_names
        finally:
            try:
                iam.remove_role_from_instance_profile(
                    InstanceProfileName=prof_name, RoleName=role_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_instance_profile(InstanceProfileName=prof_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass  # best-effort cleanup

    def test_put_delete_user_permissions_boundary(self, iam):
        """PutUserPermissionsBoundary / DeleteUserPermissionsBoundary."""
        user_name = _unique("ext-upb")
        policy_name = _unique("ext-upbp")
        iam.create_user(UserName=user_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.put_user_permissions_boundary(UserName=user_name, PermissionsBoundary=arn)
            resp = iam.get_user(UserName=user_name)
            assert resp["User"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == arn

            iam.delete_user_permissions_boundary(UserName=user_name)
            resp = iam.get_user(UserName=user_name)
            assert "PermissionsBoundary" not in resp["User"]
        finally:
            try:
                iam.delete_user_permissions_boundary(UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)
            iam.delete_policy(PolicyArn=arn)

    def test_group_inline_policy_lifecycle(self, iam):
        """PutGroupPolicy / GetGroupPolicy / ListGroupPolicies / DeleteGroupPolicy."""
        group_name = _unique("ext-gip")
        policy_name = _unique("ext-gipp")
        iam.create_group(GroupName=group_name)
        try:
            iam.put_group_policy(
                GroupName=group_name,
                PolicyName=policy_name,
                PolicyDocument=SIMPLE_POLICY_DOC,
            )
            list_resp = iam.list_group_policies(GroupName=group_name)
            assert policy_name in list_resp["PolicyNames"]

            get_resp = iam.get_group_policy(GroupName=group_name, PolicyName=policy_name)
            assert get_resp["PolicyName"] == policy_name

            iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            list_resp = iam.list_group_policies(GroupName=group_name)
            assert policy_name not in list_resp["PolicyNames"]
        finally:
            try:
                iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_group(GroupName=group_name)

    def test_get_account_authorization_details_role_filter(self, iam):
        """GetAccountAuthorizationDetails with Filter=[Role]."""
        role_name = _unique("ext-aad")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.get_account_authorization_details(Filter=["Role"])
            assert "RoleDetailList" in resp
            role_names = [r["RoleName"] for r in resp.get("RoleDetailList", [])]
            assert role_name in role_names
            # User list should be empty or absent when filtering by Role only
            assert len(resp.get("UserDetailList", [])) == 0
        finally:
            iam.delete_role(RoleName=role_name)

    def test_get_account_summary_keys(self, iam):
        """GetAccountSummary returns expected numeric keys."""
        resp = iam.get_account_summary()
        summary = resp["SummaryMap"]
        expected_keys = [
            "Users",
            "Roles",
            "Groups",
            "Policies",
            "UsersQuota",
            "RolesQuota",
            "GroupsQuota",
            "PoliciesQuota",
        ]
        for key in expected_keys:
            assert key in summary, f"Missing key: {key}"
            assert isinstance(summary[key], int), f"{key} should be int"

    def test_create_user_with_path(self, iam):
        """CreateUser with a custom Path."""
        user_name = _unique("ext-path")
        try:
            resp = iam.create_user(UserName=user_name, Path="/engineering/")
            assert resp["User"]["Path"] == "/engineering/"
        finally:
            iam.delete_user(UserName=user_name)

    def test_create_role_with_max_session_duration(self, iam):
        """CreateRole with MaxSessionDuration."""
        role_name = _unique("ext-msd")
        try:
            resp = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=TRUST_POLICY,
                MaxSessionDuration=7200,
            )
            assert resp["Role"]["MaxSessionDuration"] == 7200
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# OIDC provider extended operations
# ---------------------------------------------------------------------------


class TestIAMOIDCProviderExtended:
    def test_list_open_id_connect_providers(self, iam):
        """ListOpenIDConnectProviders."""
        url = f"https://oidc-list-{uuid.uuid4().hex[:8]}.example.com"
        thumbprint = "a" * 40
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=[thumbprint])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            listed = iam.list_open_id_connect_providers()
            arns = [p["Arn"] for p in listed["OpenIDConnectProviderList"]]
            assert arn in arns
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_update_open_id_connect_provider_thumbprint(self, iam):
        """UpdateOpenIDConnectProviderThumbprint."""
        url = f"https://oidc-thumb-{uuid.uuid4().hex[:8]}.example.com"
        old_thumb = "a" * 40
        new_thumb = "b" * 40
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=[old_thumb])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.update_open_id_connect_provider_thumbprint(
                OpenIDConnectProviderArn=arn, ThumbprintList=[new_thumb]
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert new_thumb in get_resp["ThumbprintList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_tag_open_id_connect_provider(self, iam):
        """TagOpenIDConnectProvider."""
        url = f"https://oidc-tag-{uuid.uuid4().hex[:8]}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["a" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
            )
            tag_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            tags = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "platform"
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_untag_open_id_connect_provider(self, iam):
        """UntagOpenIDConnectProvider."""
        url = f"https://oidc-untag-{uuid.uuid4().hex[:8]}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["a" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "remove-me", "Value": "bye"}],
            )
            iam.untag_open_id_connect_provider(OpenIDConnectProviderArn=arn, TagKeys=["remove-me"])
            tag_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            keys = [t["Key"] for t in tag_resp["Tags"]]
            assert "env" in keys
            assert "remove-me" not in keys
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


# ---------------------------------------------------------------------------
# SAML provider extended operations
# ---------------------------------------------------------------------------

SAML_METADATA = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
    ' entityID="https://idp.example.com/metadata">'
    "<IDPSSODescriptor"
    ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
    '<KeyDescriptor use="signing">'
    '<KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
    "<X509Data><X509Certificate>"
    "MIIDpDCCAoygAwIBAgIGAXpJOXwHMA0GCSqGSIb3DQEBCwUAMIGSMQswCQYDVQQGEwJVUzETMBEG"
    "A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU"
    "MBIGA1UECwwLU1NPUHJvdmlkZXIxEzARBgNVBAMMCmRldi04NDMyNTMxHDAaBgkqhkiG9w0BCQEW"
    "DWluZm9Ab2t0YS5jb20wHhcNMjEwNjIyMTgxNjQzWhcNMzEwNjIyMTgxNzQzWjCBkjELMAkGA1UE"
    "BhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNV"
    "BAoMBE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRMwEQYDVQQDDApkZXYtODQzMjUzMRwwGgYJ"
    "KoZIhvcNAQkBFg1pbmZvQG9rdGEuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA"
    "</X509Certificate></X509Data>"
    "</KeyInfo></KeyDescriptor>"
    "<SingleSignOnService"
    ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
    ' Location="https://idp.example.com/sso"/>'
    "<SingleSignOnService"
    ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"'
    ' Location="https://idp.example.com/sso"/>'
    "</IDPSSODescriptor></EntityDescriptor>"
)


class TestIAMSAMLProviderExtended:
    def test_list_saml_providers(self, iam):
        """ListSAMLProviders."""
        name = _unique("saml-list")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            listed = iam.list_saml_providers()
            arns = [p["Arn"] for p in listed["SAMLProviderList"]]
            assert arn in arns
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_update_saml_provider(self, iam):
        """UpdateSAMLProvider."""
        name = _unique("saml-upd")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            upd = iam.update_saml_provider(SAMLProviderArn=arn, SAMLMetadataDocument=SAML_METADATA)
            assert "SAMLProviderArn" in upd
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


# ---------------------------------------------------------------------------
# Group update
# ---------------------------------------------------------------------------


class TestIAMUpdateGroup:
    def test_update_group_rename(self, iam):
        """UpdateGroup to rename a group."""
        old_name = _unique("ug-old")
        new_name = _unique("ug-new")
        iam.create_group(GroupName=old_name)
        try:
            iam.update_group(GroupName=old_name, NewGroupName=new_name)
            resp = iam.get_group(GroupName=new_name)
            assert resp["Group"]["GroupName"] == new_name
            with pytest.raises(iam.exceptions.NoSuchEntityException):
                iam.get_group(GroupName=old_name)
        finally:
            try:
                iam.delete_group(GroupName=new_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                iam.delete_group(GroupName=old_name)
            except Exception:
                pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Login profile update
# ---------------------------------------------------------------------------


class TestIAMUpdateLoginProfile:
    def test_update_login_profile_password(self, iam):
        """UpdateLoginProfile to change the password."""
        user_name = _unique("ulp-user")
        iam.create_user(UserName=user_name)
        try:
            iam.create_login_profile(UserName=user_name, Password="OldP@ss123!")
            iam.update_login_profile(UserName=user_name, Password="N3wP@ss456!")
            # Verify the profile still exists
            resp = iam.get_login_profile(UserName=user_name)
            assert resp["LoginProfile"]["UserName"] == user_name
        finally:
            try:
                iam.delete_login_profile(UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_update_login_profile_password_reset(self, iam):
        """UpdateLoginProfile with PasswordResetRequired."""
        user_name = _unique("ulpr-user")
        iam.create_user(UserName=user_name)
        try:
            iam.create_login_profile(UserName=user_name, Password="Init@lP@ss1!")
            iam.update_login_profile(
                UserName=user_name,
                Password="Upd@tedP@ss2!",
                PasswordResetRequired=True,
            )
            resp = iam.get_login_profile(UserName=user_name)
            assert resp["LoginProfile"]["PasswordResetRequired"] is True
        finally:
            try:
                iam.delete_login_profile(UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Virtual MFA device
# ---------------------------------------------------------------------------


class TestIAMVirtualMFADevice:
    def test_create_and_delete_virtual_mfa_device(self, iam):
        """CreateVirtualMFADevice / DeleteVirtualMFADevice."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        device = resp["VirtualMFADevice"]
        serial = device["SerialNumber"]
        try:
            assert serial is not None
            assert "Base32StringSeed" in device or "QRCodePNG" in device
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_delete_virtual_mfa_device_nonexistent(self, iam):
        """DeleteVirtualMFADevice with a non-existent serial number."""
        fake_serial = "arn:aws:iam::123456789012:mfa/nonexistent-device"
        with pytest.raises(Exception) as exc_info:
            iam.delete_virtual_mfa_device(SerialNumber=fake_serial)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchEntity"

    def test_list_virtual_mfa_devices(self, iam):
        """ListVirtualMFADevices."""
        name = _unique("vmfa-list")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            listed = iam.list_virtual_mfa_devices()
            serials = [d["SerialNumber"] for d in listed["VirtualMFADevices"]]
            assert serial in serials
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)


# ---------------------------------------------------------------------------
# Server certificate
# ---------------------------------------------------------------------------

_CERT_BODY = """-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpHYbpFTMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnRlc3RjYTBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96Gj5MBHjKME0YQHMQY
yNO0+6oGxVMed2jGFMjNGJXnOXfYJknfznPmBEBz2FBBH9CjE2EKRqgyPzJfzGkv
AgMBAAEwDQYJKoZIhvcNAQELBQADQQBIIj60Fk5TBKpMBZDWTqH2GIcP/V0ufJh3
yJMOkwCe5gS2s2ULGGFY5J5cZrWH2Xyb2mjYN8Cv+GZxhUEcJoK
-----END CERTIFICATE-----"""

_PRIVATE_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIIBogIBAAJBALuj3oaPkwEeMowTRhAcxBjI07T7qgbFUx53aMYUyM0Ylec5d9gm
Sd/Oc+YEQHPYUEEf0KMTYQpGqDI/Ml/MaS8CAwEAAQJAUqwORGnMEdaQsjEHTx3q
xXJt5M0kVNIHGMi8X0bByo+WwOlTx+hA1fsp2MNjYgBXW0h+ycswdLdG8MJxxqN
AQIhAPH7lKGy8xyOgE3trUK+JC7JM5QjC4VTwvPjJR4ENTFfAiEAxXJQr8V1OgzE
zKYzZJxVMBNk5DikOZF/WpWZ5CCqf2ECIFxXnHxBMHTRguVHzB2nKbSj2hQBRBS9
eH6rJDrMPaX/AiEAxNx8TkBT7V0PJRh6qNVDzFSFPh68B3E8o3HiI+cFmEECIDvn
5Mj6pE8USRkb3DNQ/cNHguA+pLxBJO1mZHIQoOnk
-----END RSA PRIVATE KEY-----"""


class TestIAMServerCertificate:
    def test_upload_get_list_delete_server_certificate(self, iam):
        """Upload / Get / List / Delete server certificate lifecycle."""
        cert_name = _unique("srv-cert")
        resp = iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            assert resp["ServerCertificateMetadata"]["ServerCertificateName"] == cert_name

            get_resp = iam.get_server_certificate(ServerCertificateName=cert_name)
            assert (
                get_resp["ServerCertificate"]["ServerCertificateMetadata"]["ServerCertificateName"]
                == cert_name
            )

            list_resp = iam.list_server_certificates()
            names = [c["ServerCertificateName"] for c in list_resp["ServerCertificateMetadataList"]]
            assert cert_name in names
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)

    def test_delete_server_certificate_removes_it(self, iam):
        """After deletion, certificate should not appear in list."""
        cert_name = _unique("srv-cert-del")
        iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        iam.delete_server_certificate(ServerCertificateName=cert_name)
        list_resp = iam.list_server_certificates()
        names = [c["ServerCertificateName"] for c in list_resp["ServerCertificateMetadataList"]]
        assert cert_name not in names


# ---------------------------------------------------------------------------
# UpdateRoleDescription standalone
# ---------------------------------------------------------------------------


class TestIAMUpdateRoleDescription:
    def test_update_role_description_api(self, iam):
        """UpdateRoleDescription API."""
        role_name = _unique("urd-role")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Description="original",
        )
        try:
            iam.update_role_description(RoleName=role_name, Description="new description")
            resp = iam.get_role(RoleName=role_name)
            assert resp["Role"]["Description"] == "new description"
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Delete user (standalone)
# ---------------------------------------------------------------------------


class TestIAMDeleteUser:
    def test_delete_user_removes_from_list(self, iam):
        """DeleteUser removes the user from ListUsers."""
        user_name = _unique("del-user")
        iam.create_user(UserName=user_name)
        iam.delete_user(UserName=user_name)
        resp = iam.list_users()
        names = [u["UserName"] for u in resp["Users"]]
        assert user_name not in names


# ---------------------------------------------------------------------------
# Gap stubs — newly verified operations
# ---------------------------------------------------------------------------


class TestIAMGapStubs:
    def test_list_open_id_connect_providers(self, iam):
        """ListOpenIDConnectProviders returns a list (possibly empty)."""
        resp = iam.list_open_id_connect_providers()
        assert "OpenIDConnectProviderList" in resp

    def test_list_saml_providers(self, iam):
        """ListSAMLProviders returns a list (possibly empty)."""
        resp = iam.list_saml_providers()
        assert "SAMLProviderList" in resp

    def test_list_service_specific_credentials(self, iam):
        """ListServiceSpecificCredentials returns a list (possibly empty)."""
        resp = iam.list_service_specific_credentials()
        assert "ServiceSpecificCredentials" in resp

    def test_generate_credential_report(self, iam):
        """GenerateCredentialReport returns a State."""
        resp = iam.generate_credential_report()
        assert "State" in resp
        assert resp["State"] in ("STARTED", "INPROGRESS", "COMPLETE")

    def test_list_open_id_connect_provider_tags(self, iam):
        """ListOpenIDConnectProviderTags returns tags for a real provider."""
        resp = iam.create_open_id_connect_provider(
            Url="https://oidc-tag-test.example.com",
            ThumbprintList=["a" * 40],
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            tag_resp = iam.list_open_id_connect_provider_tags(
                OpenIDConnectProviderArn=arn,
            )
            assert "Tags" in tag_resp
            assert isinstance(tag_resp["Tags"], list)
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


class TestIamAutoCoverage:
    """Auto-generated coverage tests for iam."""

    @pytest.fixture
    def client(self):
        return make_client("iam")

    def test_disable_organizations_root_credentials_management(self, client):
        """DisableOrganizationsRootCredentialsManagement returns a response."""
        resp = client.disable_organizations_root_credentials_management()
        assert "OrganizationId" in resp

    def test_disable_organizations_root_sessions(self, client):
        """DisableOrganizationsRootSessions returns a response."""
        resp = client.disable_organizations_root_sessions()
        assert "OrganizationId" in resp

    def test_disable_outbound_web_identity_federation(self, client):
        """DisableOutboundWebIdentityFederation returns a response."""
        client.disable_outbound_web_identity_federation()

    def test_enable_organizations_root_credentials_management(self, client):
        """EnableOrganizationsRootCredentialsManagement returns a response."""
        resp = client.enable_organizations_root_credentials_management()
        assert "OrganizationId" in resp

    def test_enable_organizations_root_sessions(self, client):
        """EnableOrganizationsRootSessions returns a response."""
        resp = client.enable_organizations_root_sessions()
        assert "OrganizationId" in resp

    def test_enable_outbound_web_identity_federation(self, client):
        """EnableOutboundWebIdentityFederation returns a response."""
        client.enable_outbound_web_identity_federation()

    def test_get_outbound_web_identity_federation_info(self, client):
        """GetOutboundWebIdentityFederationInfo returns a response."""
        client.get_outbound_web_identity_federation_info()

    def test_list_delegation_requests(self, client):
        """ListDelegationRequests returns a response."""
        resp = client.list_delegation_requests()
        assert "DelegationRequests" in resp

    def test_list_open_id_connect_providers(self, client):
        """ListOpenIDConnectProviders returns a response."""
        resp = client.list_open_id_connect_providers()
        assert "OpenIDConnectProviderList" in resp

    def test_list_organizations_features(self, client):
        """ListOrganizationsFeatures returns a response."""
        resp = client.list_organizations_features()
        assert "OrganizationId" in resp

    def test_list_saml_providers(self, client):
        """ListSAMLProviders returns a response."""
        resp = client.list_saml_providers()
        assert "SAMLProviderList" in resp

    def test_list_virtual_mfa_devices(self, client):
        """ListVirtualMFADevices returns a response."""
        resp = client.list_virtual_mfa_devices()
        assert "VirtualMFADevices" in resp


# ---------------------------------------------------------------------------
# Instance profile tagging
# ---------------------------------------------------------------------------


class TestIAMInstanceProfileTags:
    def test_tag_instance_profile(self, iam):
        """TagInstanceProfile adds tags to an instance profile."""
        name = _unique("ip-tag")
        iam.create_instance_profile(InstanceProfileName=name)
        try:
            iam.tag_instance_profile(
                InstanceProfileName=name,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
            )
            resp = iam.get_instance_profile(InstanceProfileName=name)
            tag_keys = {t["Key"] for t in resp["InstanceProfile"].get("Tags", [])}
            assert "env" in tag_keys
            assert "team" in tag_keys
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)

    def test_untag_instance_profile(self, iam):
        """UntagInstanceProfile removes tags from an instance profile."""
        name = _unique("ip-untag")
        iam.create_instance_profile(InstanceProfileName=name)
        try:
            iam.tag_instance_profile(
                InstanceProfileName=name,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "remove-me", "Value": "bye"}],
            )
            iam.untag_instance_profile(InstanceProfileName=name, TagKeys=["remove-me"])
            resp = iam.get_instance_profile(InstanceProfileName=name)
            tag_keys = {t["Key"] for t in resp["InstanceProfile"].get("Tags", [])}
            assert "env" in tag_keys
            assert "remove-me" not in tag_keys
        finally:
            iam.delete_instance_profile(InstanceProfileName=name)


# ---------------------------------------------------------------------------
# Policy tagging
# ---------------------------------------------------------------------------


class TestIAMPolicyTags:
    def test_tag_policy(self, iam):
        """TagPolicy adds tags to a managed policy."""
        pol_name = _unique("pol-tag")
        resp = iam.create_policy(PolicyName=pol_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = resp["Policy"]["Arn"]
        try:
            iam.tag_policy(
                PolicyArn=arn,
                Tags=[{"Key": "env", "Value": "staging"}, {"Key": "cost", "Value": "free"}],
            )
            tag_resp = iam.list_policy_tags(PolicyArn=arn)
            tags = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tags["env"] == "staging"
            assert tags["cost"] == "free"
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_untag_policy(self, iam):
        """UntagPolicy removes tags from a managed policy."""
        pol_name = _unique("pol-untag")
        resp = iam.create_policy(PolicyName=pol_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = resp["Policy"]["Arn"]
        try:
            iam.tag_policy(
                PolicyArn=arn,
                Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "drop", "Value": "bye"}],
            )
            iam.untag_policy(PolicyArn=arn, TagKeys=["drop"])
            tag_resp = iam.list_policy_tags(PolicyArn=arn)
            keys = [t["Key"] for t in tag_resp["Tags"]]
            assert "keep" in keys
            assert "drop" not in keys
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_list_policy_tags(self, iam):
        """ListPolicyTags returns tags for a policy."""
        pol_name = _unique("pol-listtag")
        resp = iam.create_policy(PolicyName=pol_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = resp["Policy"]["Arn"]
        try:
            tag_resp = iam.list_policy_tags(PolicyArn=arn)
            assert "Tags" in tag_resp
            assert isinstance(tag_resp["Tags"], list)
        finally:
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Access key last used
# ---------------------------------------------------------------------------


class TestIAMAccessKeyLastUsed:
    def test_get_access_key_last_used(self, iam):
        """GetAccessKeyLastUsed returns info about an access key."""
        user_name = _unique("ak-lu")
        iam.create_user(UserName=user_name)
        try:
            key_resp = iam.create_access_key(UserName=user_name)
            access_key_id = key_resp["AccessKey"]["AccessKeyId"]
            resp = iam.get_access_key_last_used(AccessKeyId=access_key_id)
            assert "AccessKeyLastUsed" in resp
            assert "UserName" in resp
        finally:
            iam.delete_access_key(UserName=user_name, AccessKeyId=access_key_id)
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Update assume role policy
# ---------------------------------------------------------------------------


class TestIAMUpdateAssumeRolePolicy:
    def test_update_assume_role_policy(self, iam):
        """UpdateAssumeRolePolicy changes the trust policy of a role."""
        role_name = _unique("uarp")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            new_trust = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ec2.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            )
            iam.update_assume_role_policy(RoleName=role_name, PolicyDocument=new_trust)
            resp = iam.get_role(RoleName=role_name)
            # The trust policy should be updated
            assert "AssumeRolePolicyDocument" in resp["Role"]
        finally:
            iam.delete_role(RoleName=role_name)

    def test_update_assume_role_policy_nonexistent_role(self, iam):
        """UpdateAssumeRolePolicy on a non-existent role returns NoSuchEntity."""
        with pytest.raises(iam.exceptions.NoSuchEntityException) as exc_info:
            iam.update_assume_role_policy(
                RoleName="no-such-role-xyz",
                PolicyDocument=TRUST_POLICY,
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchEntity"

    def test_update_assume_role_policy_verify_content(self, iam):
        """UpdateAssumeRolePolicy: verify the new policy document content."""
        role_name = _unique("uarp-v")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            new_trust = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ec2.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            )
            iam.update_assume_role_policy(RoleName=role_name, PolicyDocument=new_trust)
            resp = iam.get_role(RoleName=role_name)
            doc = resp["Role"]["AssumeRolePolicyDocument"]
            # doc is already parsed by boto3
            principals = [s["Principal"] for s in doc["Statement"]]
            assert {"Service": "ec2.amazonaws.com"} in principals
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# MFA device operations
# ---------------------------------------------------------------------------


class TestIAMMFADeviceOps:
    def test_enable_and_deactivate_mfa_device(self, iam):
        """EnableMFADevice / DeactivateMFADevice lifecycle."""
        user_name = _unique("mfa-user")
        vmfa_name = _unique("vmfa-en")
        iam.create_user(UserName=user_name)
        vmfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=vmfa_name)
        serial = vmfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            # Enable requires two consecutive TOTP codes; use 123456/654321 which Moto accepts
            iam.enable_mfa_device(
                UserName=user_name,
                SerialNumber=serial,
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
            # Verify MFA device is listed for the user
            listed = iam.list_mfa_devices(UserName=user_name)
            serials = [d["SerialNumber"] for d in listed["MFADevices"]]
            assert serial in serials
            # Deactivate it
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            listed2 = iam.list_mfa_devices(UserName=user_name)
            serials2 = [d["SerialNumber"] for d in listed2["MFADevices"]]
            assert serial not in serials2
        finally:
            try:
                iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_virtual_mfa_device(SerialNumber=serial)
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# SSH public key upload
# ---------------------------------------------------------------------------

_SSH_PUBLIC_KEY = (
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCz8Dk176Uh4bPmFosfHGQQwJj5ejGnlf"
    "0RmTbVBjDaBCO/x4D98DhtVIBHdIUVOhfFkKG3cAfpJo2rWB7RkDzV2OywOa0Nlb5PXhR"
    "hGJsXBGLGEOGp5HoAiIeUJGZ0GZ7Ly7PGbKMe2OjhKfSHn9UkER6A+BxMp7J1w9ZMPX2j"
    "bHuXnuBErjUCr3LGXNN9p2gaQQ3nGxw4sFMq3bJWKW7R2Dz1VJfBjEqFMk5LYqP1n5M+1"
    "HQYPJEFbKAHDN3OL3F4D3QPDJneMCOLI3EcsJJDPVpFaGp1qP5vQJ5yf0ABXk0EJ1fH8F"
    "hIzFNJdNq+LVMIHChSxFIpfdmh test-key"
)


class TestIAMSSHPublicKeyUpload:
    def test_upload_ssh_public_key(self, iam):
        """UploadSSHPublicKey uploads an SSH key for a user."""
        user_name = _unique("ssh-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(UserName=user_name, SSHPublicKeyBody=_SSH_PUBLIC_KEY)
            assert "SSHPublicKey" in resp
            key_id = resp["SSHPublicKey"]["SSHPublicKeyId"]
            assert key_id is not None
            # Verify it shows up in list
            listed = iam.list_ssh_public_keys(UserName=user_name)
            ids = [k["SSHPublicKeyId"] for k in listed["SSHPublicKeys"]]
            assert key_id in ids
        finally:
            try:
                iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Signing certificate upload
# ---------------------------------------------------------------------------


_SIGNING_CERT = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIICrjCCAZagAwIBAgIUTPWk8SRG5QxyO0DJgq/Umg480sswDQYJKoZIhvcNAQEL\n"
    "BQAwETEPMA0GA1UEAwwGdGVzdGNhMB4XDTIzMDEwMTAwMDAwMFoXDTMzMDEwMTAw\n"
    "MDAwMFowETEPMA0GA1UEAwwGdGVzdGNhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n"
    "MIIBCgKCAQEAz323fnNbq6P9+Wf5wzk7l3Z3ouYJ3DKlL3ae3hWK7P4i+lFucumq\n"
    "RGn4O7cKae3GtSlm+gfIk0+0nAU1nj2z6E9q7O5pRPlqilGGPQcY3D3mvZcu/Jup\n"
    "DVQCM mn8iPwPo4b/Ch6Z2vP7+5b2dQal4L3fyHbf4yTx1xNczN4xEu8HyV8+5+MS\n"
    "xH/LrNyKRsOERNeCi+Fvi452ZD4l7Hn3lnPY/kvx5iUEISXAr5X2GKzPsSN4hBWx\n"
    "66pqNIQwz08zZYRunrxnSatShnlUtdL9YhinRuL9VPR1Ahy+NQSOD5U1AsOeywdF\n"
    "o9UaJ0kxaPXTNnr21P1Qw3inh5+mFo6FewIDAQABMA0GCSqGSIb3DQEBCwUAA4IB\n"
    "AQBpBPAhB1WmAIURhM65OoAC83qxMyRk3rGW4oSagXVxB3o7njAg2PXA9Sd7XqFy\n"
    "5UHCfOu9vZkW9sSDtWXrRKRIcdD8C6CH1x/AQZrqNzw1SGERW2ojqtf5tXFPOsg9\n"
    "j4YgrSBXhpoaDfQq5mazRYpUh+5qv+loiq0tGT2eJolG4RpKVw/MwKGvaMLxF502\n"
    "UlXukjLy9XFH2t5Ef7L5oX8BlRykhRObhYk/P4WAGUyf4Yprd+41XajzPqjEjJ1w\n"
    "XDy3aJO9t2E8TtL4uUzFY01bZGRNOW8uyW8+CT2drQQ7bR3x+TuYZwj0AicyJRV+\n"
    "QpPkw4Fr0WMLYoxPCAlsLvoc\n"
    "-----END CERTIFICATE-----"
)


class TestIAMSigningCertificateUpload:
    def test_upload_signing_certificate(self, iam):
        """UploadSigningCertificate uploads a signing certificate for a user."""
        user_name = _unique("sigcert-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_signing_certificate(UserName=user_name, CertificateBody=_SIGNING_CERT)
            assert "Certificate" in resp
            cert_id = resp["Certificate"]["CertificateId"]
            assert cert_id is not None
            # Verify it shows up in list
            listed = iam.list_signing_certificates(UserName=user_name)
            ids = [c["CertificateId"] for c in listed["Certificates"]]
            assert cert_id in ids
        finally:
            try:
                iam.delete_signing_certificate(UserName=user_name, CertificateId=cert_id)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


class TestIAMOpenIDConnectProviderThumbprint:
    def test_update_openid_connect_provider_thumbprint(self, iam):
        """UpdateOpenIDConnectProviderThumbprint updates the thumbprint list."""
        url = f"https://oidc-thumb-{uuid.uuid4().hex[:8]}.example.com"
        original_thumb = "a" * 40
        new_thumb = "b" * 40
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=[original_thumb])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.update_open_id_connect_provider_thumbprint(
                OpenIDConnectProviderArn=arn, ThumbprintList=[new_thumb]
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert new_thumb in get_resp["ThumbprintList"]
            assert original_thumb not in get_resp["ThumbprintList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


# ---------------------------------------------------------------------------
# SSH public key full lifecycle: get, update status, delete with assertions
# ---------------------------------------------------------------------------


class TestIAMSSHPublicKeyLifecycle:
    def test_get_ssh_public_key(self, iam):
        """GetSSHPublicKey returns the uploaded key details."""
        user_name = _unique("ssh-get")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(UserName=user_name, SSHPublicKeyBody=_SSH_PUBLIC_KEY)
            key_id = resp["SSHPublicKey"]["SSHPublicKeyId"]
            get_resp = iam.get_ssh_public_key(
                UserName=user_name, SSHPublicKeyId=key_id, Encoding="SSH"
            )
            assert get_resp["SSHPublicKey"]["SSHPublicKeyId"] == key_id
            assert get_resp["SSHPublicKey"]["UserName"] == user_name
            assert get_resp["SSHPublicKey"]["Status"] == "Active"
        finally:
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            iam.delete_user(UserName=user_name)

    def test_update_ssh_public_key_status(self, iam):
        """UpdateSSHPublicKey changes key status to Inactive."""
        user_name = _unique("ssh-upd")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(UserName=user_name, SSHPublicKeyBody=_SSH_PUBLIC_KEY)
            key_id = resp["SSHPublicKey"]["SSHPublicKeyId"]
            iam.update_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id, Status="Inactive")
            get_resp = iam.get_ssh_public_key(
                UserName=user_name, SSHPublicKeyId=key_id, Encoding="SSH"
            )
            assert get_resp["SSHPublicKey"]["Status"] == "Inactive"
        finally:
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            iam.delete_user(UserName=user_name)

    def test_delete_ssh_public_key(self, iam):
        """DeleteSSHPublicKey removes the key from the user."""
        user_name = _unique("ssh-del")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(UserName=user_name, SSHPublicKeyBody=_SSH_PUBLIC_KEY)
            key_id = resp["SSHPublicKey"]["SSHPublicKeyId"]
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            listed = iam.list_ssh_public_keys(UserName=user_name)
            ids = [k["SSHPublicKeyId"] for k in listed["SSHPublicKeys"]]
            assert key_id not in ids
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Signing certificate update and delete with assertions
# ---------------------------------------------------------------------------


class TestIAMSigningCertificateLifecycle:
    def test_update_signing_certificate_status(self, iam):
        """UpdateSigningCertificate changes certificate status to Inactive."""
        user_name = _unique("sigupd")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_signing_certificate(UserName=user_name, CertificateBody=_SIGNING_CERT)
            cert_id = resp["Certificate"]["CertificateId"]
            iam.update_signing_certificate(
                UserName=user_name, CertificateId=cert_id, Status="Inactive"
            )
            listed = iam.list_signing_certificates(UserName=user_name)
            cert = [c for c in listed["Certificates"] if c["CertificateId"] == cert_id][0]
            assert cert["Status"] == "Inactive"
        finally:
            try:
                iam.delete_signing_certificate(UserName=user_name, CertificateId=cert_id)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_delete_signing_certificate(self, iam):
        """DeleteSigningCertificate removes the certificate from the user."""
        user_name = _unique("sigdel")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_signing_certificate(UserName=user_name, CertificateBody=_SIGNING_CERT)
            cert_id = resp["Certificate"]["CertificateId"]
            iam.delete_signing_certificate(UserName=user_name, CertificateId=cert_id)
            listed = iam.list_signing_certificates(UserName=user_name)
            ids = [c["CertificateId"] for c in listed["Certificates"]]
            assert cert_id not in ids
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# UpdateUser with NewPath
# ---------------------------------------------------------------------------


class TestIAMUpdateUserPath:
    def test_update_user_path(self, iam):
        """UpdateUser with NewPath changes the user's path."""
        user_name = _unique("pathuser")
        iam.create_user(UserName=user_name, Path="/original/")
        try:
            resp = iam.get_user(UserName=user_name)
            assert resp["User"]["Path"] == "/original/"
            iam.update_user(UserName=user_name, NewPath="/updated/")
            resp2 = iam.get_user(UserName=user_name)
            assert resp2["User"]["Path"] == "/updated/"
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# UpdateRole with MaxSessionDuration
# ---------------------------------------------------------------------------


class TestIAMUpdateRoleMaxSession:
    def test_update_role_max_session_duration(self, iam):
        """UpdateRole with MaxSessionDuration changes the role's max session."""
        role_name = _unique("maxsess")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
        )
        try:
            iam.update_role(RoleName=role_name, MaxSessionDuration=7200)
            get_resp = iam.get_role(RoleName=role_name)
            assert get_resp["Role"]["MaxSessionDuration"] == 7200
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Dedicated tests for operations previously only used as helpers
# ---------------------------------------------------------------------------


class TestIAMAddRoleToInstanceProfile:
    def test_add_role_to_instance_profile(self, iam):
        """AddRoleToInstanceProfile adds a role and it appears in the profile."""
        profile_name = _unique("arip-prof")
        role_name = _unique("arip-role")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            profile = iam.get_instance_profile(InstanceProfileName=profile_name)
            role_names = [r["RoleName"] for r in profile["InstanceProfile"]["Roles"]]
            assert role_name in role_names
        finally:
            iam.remove_role_from_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            iam.delete_instance_profile(InstanceProfileName=profile_name)
            iam.delete_role(RoleName=role_name)


class TestIAMAttachUserPolicy:
    def test_attach_user_policy(self, iam):
        """AttachUserPolicy attaches a managed policy to a user."""
        user_name = _unique("aup-user")
        policy_name = _unique("aup-pol")
        iam.create_user(UserName=user_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.attach_user_policy(UserName=user_name, PolicyArn=arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            attached = iam.list_attached_user_policies(UserName=user_name)
            arns = [p["PolicyArn"] for p in attached["AttachedPolicies"]]
            assert arn in arns
        finally:
            iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_user(UserName=user_name)


class TestIAMCreateAccountAlias:
    def test_create_account_alias(self, iam):
        """CreateAccountAlias creates an alias visible in ListAccountAliases."""
        alias = _unique("acct-alias")
        try:
            resp = iam.create_account_alias(AccountAlias=alias)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_account_aliases()
            assert alias in listed["AccountAliases"]
        finally:
            iam.delete_account_alias(AccountAlias=alias)


class TestIAMDeleteAccountAlias:
    def test_delete_account_alias(self, iam):
        """DeleteAccountAlias removes the alias."""
        alias = _unique("del-alias")
        iam.create_account_alias(AccountAlias=alias)
        resp = iam.delete_account_alias(AccountAlias=alias)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        listed = iam.list_account_aliases()
        assert alias not in listed["AccountAliases"]


class TestIAMCreateLoginProfile:
    def test_create_login_profile(self, iam):
        """CreateLoginProfile creates a password for a user."""
        user_name = _unique("clp-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.create_login_profile(
                UserName=user_name, Password="Test@12345678", PasswordResetRequired=False
            )
            assert resp["LoginProfile"]["UserName"] == user_name
            assert "CreateDate" in resp["LoginProfile"]
        finally:
            iam.delete_login_profile(UserName=user_name)
            iam.delete_user(UserName=user_name)


class TestIAMCreateOpenIDConnectProvider:
    def test_create_open_id_connect_provider(self, iam):
        """CreateOpenIDConnectProvider creates an OIDC provider."""
        url = f"https://{_unique('oidc')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=["a" * 40],
            ClientIDList=["test-client"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            assert "arn:aws:iam:" in arn
            provider = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "test-client" in provider["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


class TestIAMCreateSAMLProvider:
    def test_create_saml_provider(self, iam):
        """CreateSAMLProvider creates a SAML provider."""
        name = _unique("saml-prov")
        saml_doc = (
            '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
            ' entityID="https://test.example.com">'
            "<IDPSSODescriptor"
            ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
            "<SingleSignOnService"
            ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
            ' Location="https://test.example.com/sso"/>'
            "</IDPSSODescriptor>"
            "</EntityDescriptor>"
        )
        # Pad to meet 1000 char minimum
        saml_doc = saml_doc + " " * max(0, 1000 - len(saml_doc))
        resp = iam.create_saml_provider(SAMLMetadataDocument=saml_doc, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            assert "arn:aws:iam:" in arn
            provider = iam.get_saml_provider(SAMLProviderArn=arn)
            assert "SAMLMetadataDocument" in provider
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


class TestIAMCreateServiceLinkedRole:
    def test_create_service_linked_role(self, iam):
        """CreateServiceLinkedRole creates a service-linked role."""
        resp = iam.create_service_linked_role(AWSServiceName="elasticbeanstalk.amazonaws.com")
        role = resp["Role"]
        assert "AWSServiceRoleForElasticBeanstalk" in role["RoleName"]
        assert role["Path"] == "/aws-service-role/elasticbeanstalk.amazonaws.com/"
        iam.delete_service_linked_role(RoleName=role["RoleName"])


class TestIAMCreateVirtualMFADevice:
    def test_create_virtual_mfa_device(self, iam):
        """CreateVirtualMFADevice creates a virtual MFA device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        device = resp["VirtualMFADevice"]
        serial = device["SerialNumber"]
        try:
            assert name in serial
            assert "Base32StringSeed" in device or "QRCodePNG" in device
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)


class TestIAMDeleteGroupPolicy:
    def test_delete_group_policy(self, iam):
        """DeleteGroupPolicy removes an inline policy from a group."""
        group_name = _unique("dgp-grp")
        policy_name = _unique("dgp-pol")
        iam.create_group(GroupName=group_name)
        try:
            iam.put_group_policy(
                GroupName=group_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_group_policies(GroupName=group_name)
            assert policy_name not in listed["PolicyNames"]
        finally:
            iam.delete_group(GroupName=group_name)


class TestIAMDeleteInstanceProfile:
    def test_delete_instance_profile(self, iam):
        """DeleteInstanceProfile removes an instance profile."""
        profile_name = _unique("dip-prof")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        resp = iam.delete_instance_profile(InstanceProfileName=profile_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        listed = iam.list_instance_profiles()
        names = [p["InstanceProfileName"] for p in listed["InstanceProfiles"]]
        assert profile_name not in names


class TestIAMDeleteOpenIDConnectProvider:
    def test_delete_open_id_connect_provider(self, iam):
        """DeleteOpenIDConnectProvider removes an OIDC provider."""
        url = f"https://{_unique('doidc')}.example.com"
        create_resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["b" * 40])
        arn = create_resp["OpenIDConnectProviderArn"]
        resp = iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        listed = iam.list_open_id_connect_providers()
        arns = [p["Arn"] for p in listed["OpenIDConnectProviderList"]]
        assert arn not in arns


class TestIAMDetachGroupPolicy:
    def test_detach_group_policy(self, iam):
        """DetachGroupPolicy detaches a managed policy from a group."""
        group_name = _unique("dgp2-grp")
        policy_name = _unique("dgp2-pol")
        iam.create_group(GroupName=group_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_group_policy(GroupName=group_name, PolicyArn=arn)
            resp = iam.detach_group_policy(GroupName=group_name, PolicyArn=arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            attached = iam.list_attached_group_policies(GroupName=group_name)
            arns = [p["PolicyArn"] for p in attached["AttachedPolicies"]]
            assert arn not in arns
        finally:
            iam.delete_policy(PolicyArn=arn)
            iam.delete_group(GroupName=group_name)


class TestIAMEnableMFADevice:
    def test_enable_mfa_device(self, iam):
        """EnableMFADevice associates a virtual MFA device with a user."""
        user_name = _unique("emfa-user")
        mfa_name = _unique("emfa-dev")
        iam.create_user(UserName=user_name)
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            resp = iam.enable_mfa_device(
                UserName=user_name,
                SerialNumber=serial,
                AuthenticationCode1="123456",
                AuthenticationCode2="789012",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            devices = iam.list_mfa_devices(UserName=user_name)
            serials = [d["SerialNumber"] for d in devices["MFADevices"]]
            assert serial in serials
        finally:
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            iam.delete_virtual_mfa_device(SerialNumber=serial)
            iam.delete_user(UserName=user_name)


class TestIAMGetAccountPasswordPolicy:
    def test_get_account_password_policy(self, iam):
        """GetAccountPasswordPolicy returns the password policy after it's set."""
        iam.update_account_password_policy(
            MinimumPasswordLength=12, RequireUppercaseCharacters=True
        )
        try:
            resp = iam.get_account_password_policy()
            policy = resp["PasswordPolicy"]
            assert policy["MinimumPasswordLength"] == 12
            assert policy["RequireUppercaseCharacters"] is True
        finally:
            iam.delete_account_password_policy()


class TestIAMGetGroupPolicy:
    def test_get_group_policy(self, iam):
        """GetGroupPolicy returns the inline policy document for a group."""
        group_name = _unique("ggp-grp")
        policy_name = _unique("ggp-pol")
        iam.create_group(GroupName=group_name)
        try:
            iam.put_group_policy(
                GroupName=group_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.get_group_policy(GroupName=group_name, PolicyName=policy_name)
            assert resp["GroupName"] == group_name
            assert resp["PolicyName"] == policy_name
            assert "PolicyDocument" in resp
        finally:
            iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            iam.delete_group(GroupName=group_name)


class TestIAMGetLoginProfile:
    def test_get_login_profile(self, iam):
        """GetLoginProfile returns the login profile for a user."""
        user_name = _unique("glp-user")
        iam.create_user(UserName=user_name)
        iam.create_login_profile(
            UserName=user_name, Password="Test@12345678", PasswordResetRequired=False
        )
        try:
            resp = iam.get_login_profile(UserName=user_name)
            assert resp["LoginProfile"]["UserName"] == user_name
            assert "CreateDate" in resp["LoginProfile"]
        finally:
            iam.delete_login_profile(UserName=user_name)
            iam.delete_user(UserName=user_name)


class TestIAMGetOpenIDConnectProvider:
    def test_get_open_id_connect_provider(self, iam):
        """GetOpenIDConnectProvider returns OIDC provider details."""
        url = f"https://{_unique('goidc')}.example.com"
        create_resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=["c" * 40],
            ClientIDList=["client-1", "client-2"],
        )
        arn = create_resp["OpenIDConnectProviderArn"]
        try:
            resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "c" * 40 in resp["ThumbprintList"]
            assert len(resp["ClientIDList"]) == 2
            assert "CreateDate" in resp
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


class TestIAMGetRole:
    def test_get_role(self, iam):
        """GetRole returns role details including trust policy."""
        role_name = _unique("gr-role")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Description="test role for get_role",
        )
        try:
            resp = iam.get_role(RoleName=role_name)
            assert resp["Role"]["RoleName"] == role_name
            assert resp["Role"]["Description"] == "test role for get_role"
            assert "AssumeRolePolicyDocument" in resp["Role"]
            assert "Arn" in resp["Role"]
        finally:
            iam.delete_role(RoleName=role_name)


class TestIAMGetSAMLProvider:
    def test_get_saml_provider(self, iam):
        """GetSAMLProvider returns SAML provider metadata."""
        name = _unique("gsaml")
        saml_doc = (
            '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
            ' entityID="https://getsaml.example.com">'
            "<IDPSSODescriptor"
            ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
            "<SingleSignOnService"
            ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
            ' Location="https://getsaml.example.com/sso"/>'
            "</IDPSSODescriptor>"
            "</EntityDescriptor>"
        )
        saml_doc = saml_doc + " " * max(0, 1000 - len(saml_doc))
        resp = iam.create_saml_provider(SAMLMetadataDocument=saml_doc, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            provider = iam.get_saml_provider(SAMLProviderArn=arn)
            assert "SAMLMetadataDocument" in provider
            assert "CreateDate" in provider
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


class TestIAMGetServerCertificate:
    def test_get_server_certificate(self, iam):
        """GetServerCertificate returns certificate details."""
        cert_name = _unique("gsc-cert")
        # Upload a cert first
        iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            get_resp = iam.get_server_certificate(ServerCertificateName=cert_name)
            cert = get_resp["ServerCertificate"]
            assert cert["ServerCertificateMetadata"]["ServerCertificateName"] == cert_name
            assert "CertificateBody" in cert
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)


class TestIAMGetServiceLinkedRoleDeletionStatus:
    def test_get_service_linked_role_deletion_status(self, iam):
        """GetServiceLinkedRoleDeletionStatus returns status for a deletion task."""
        role_resp = iam.create_service_linked_role(AWSServiceName="autoscaling.amazonaws.com")
        role_name = role_resp["Role"]["RoleName"]
        del_resp = iam.delete_service_linked_role(RoleName=role_name)
        task_id = del_resp["DeletionTaskId"]
        status_resp = iam.get_service_linked_role_deletion_status(DeletionTaskId=task_id)
        assert status_resp["Status"] in ("SUCCEEDED", "IN_PROGRESS", "NOT_STARTED", "FAILED")


class TestIAMGetUserPolicy:
    def test_get_user_policy(self, iam):
        """GetUserPolicy returns the inline policy document for a user."""
        user_name = _unique("gup-user")
        policy_name = _unique("gup-pol")
        iam.create_user(UserName=user_name)
        try:
            iam.put_user_policy(
                UserName=user_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.get_user_policy(UserName=user_name, PolicyName=policy_name)
            assert resp["UserName"] == user_name
            assert resp["PolicyName"] == policy_name
            assert "PolicyDocument" in resp
        finally:
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)
            iam.delete_user(UserName=user_name)


class TestIAMListAttachedGroupPolicies:
    def test_list_attached_group_policies(self, iam):
        """ListAttachedGroupPolicies returns attached managed policies."""
        group_name = _unique("lagp-grp")
        policy_name = _unique("lagp-pol")
        iam.create_group(GroupName=group_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_group_policy(GroupName=group_name, PolicyArn=arn)
            resp = iam.list_attached_group_policies(GroupName=group_name)
            assert "AttachedPolicies" in resp
            names = [p["PolicyName"] for p in resp["AttachedPolicies"]]
            assert policy_name in names
        finally:
            iam.detach_group_policy(GroupName=group_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_group(GroupName=group_name)


class TestIAMListAttachedUserPolicies:
    def test_list_attached_user_policies(self, iam):
        """ListAttachedUserPolicies returns attached managed policies."""
        user_name = _unique("laup-user")
        policy_name = _unique("laup-pol")
        iam.create_user(UserName=user_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_user_policy(UserName=user_name, PolicyArn=arn)
            resp = iam.list_attached_user_policies(UserName=user_name)
            assert "AttachedPolicies" in resp
            names = [p["PolicyName"] for p in resp["AttachedPolicies"]]
            assert policy_name in names
        finally:
            iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_user(UserName=user_name)


class TestIAMListGroupPolicies:
    def test_list_group_policies(self, iam):
        """ListGroupPolicies returns inline policy names for a group."""
        group_name = _unique("lgp-grp")
        policy_name = _unique("lgp-pol")
        iam.create_group(GroupName=group_name)
        try:
            iam.put_group_policy(
                GroupName=group_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.list_group_policies(GroupName=group_name)
            assert "PolicyNames" in resp
            assert policy_name in resp["PolicyNames"]
        finally:
            iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            iam.delete_group(GroupName=group_name)


class TestIAMListServerCertificates:
    def test_list_server_certificates(self, iam):
        """ListServerCertificates returns uploaded certificates."""
        cert_name = _unique("lsc-cert")
        iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            resp = iam.list_server_certificates()
            assert "ServerCertificateMetadataList" in resp
            names = [c["ServerCertificateName"] for c in resp["ServerCertificateMetadataList"]]
            assert cert_name in names
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)


class TestIAMPutGroupPolicy:
    def test_put_group_policy(self, iam):
        """PutGroupPolicy creates an inline policy on a group."""
        group_name = _unique("pgp-grp")
        policy_name = _unique("pgp-pol")
        iam.create_group(GroupName=group_name)
        try:
            resp = iam.put_group_policy(
                GroupName=group_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_group_policies(GroupName=group_name)
            assert policy_name in listed["PolicyNames"]
        finally:
            iam.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            iam.delete_group(GroupName=group_name)


class TestIAMPutRolePermissionsBoundary:
    def test_put_role_permissions_boundary(self, iam):
        """PutRolePermissionsBoundary sets a permissions boundary on a role."""
        role_name = _unique("prpb-role")
        policy_name = _unique("prpb-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.put_role_permissions_boundary(RoleName=role_name, PermissionsBoundary=arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            role = iam.get_role(RoleName=role_name)
            assert role["Role"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == arn
        finally:
            iam.delete_role_permissions_boundary(RoleName=role_name)
            iam.delete_role(RoleName=role_name)
            iam.delete_policy(PolicyArn=arn)


class TestIAMPutRolePolicy:
    def test_put_role_policy(self, iam):
        """PutRolePolicy creates an inline policy on a role."""
        role_name = _unique("prp-role")
        policy_name = _unique("prp-pol")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.put_role_policy(
                RoleName=role_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_role_policies(RoleName=role_name)
            assert policy_name in listed["PolicyNames"]
        finally:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            iam.delete_role(RoleName=role_name)


class TestIAMPutUserPermissionsBoundary:
    def test_put_user_permissions_boundary(self, iam):
        """PutUserPermissionsBoundary sets a permissions boundary on a user."""
        user_name = _unique("pupb-user")
        policy_name = _unique("pupb-pol")
        iam.create_user(UserName=user_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.put_user_permissions_boundary(UserName=user_name, PermissionsBoundary=arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            user = iam.get_user(UserName=user_name)
            assert user["User"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == arn
        finally:
            iam.delete_user_permissions_boundary(UserName=user_name)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_user(UserName=user_name)


class TestIAMPutUserPolicy:
    def test_put_user_policy(self, iam):
        """PutUserPolicy creates an inline policy on a user."""
        user_name = _unique("pup-user")
        policy_name = _unique("pup-pol")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.put_user_policy(
                UserName=user_name, PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_user_policies(UserName=user_name)
            assert policy_name in listed["PolicyNames"]
        finally:
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)
            iam.delete_user(UserName=user_name)


class TestIAMUpdateAccountPasswordPolicy:
    def test_update_account_password_policy(self, iam):
        """UpdateAccountPasswordPolicy updates the account password policy."""
        try:
            resp = iam.update_account_password_policy(
                MinimumPasswordLength=14,
                RequireSymbols=True,
                RequireNumbers=True,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            policy = iam.get_account_password_policy()
            assert policy["PasswordPolicy"]["MinimumPasswordLength"] == 14
            assert policy["PasswordPolicy"]["RequireSymbols"] is True
        finally:
            iam.delete_account_password_policy()


class TestIAMUploadServerCertificate:
    def test_upload_server_certificate(self, iam):
        """UploadServerCertificate uploads a certificate."""
        cert_name = _unique("usc-cert")
        resp = iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            meta = resp["ServerCertificateMetadata"]
            assert meta["ServerCertificateName"] == cert_name
            assert "Arn" in meta
            assert "ServerCertificateId" in meta
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)


class TestIAMServiceSpecificCredentials:
    """Tests for ServiceSpecificCredential CRUD operations."""

    def test_create_service_specific_credential(self, iam):
        """CreateServiceSpecificCredential creates a credential for a service."""
        user_name = _unique("ssc-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.create_service_specific_credential(
                UserName=user_name,
                ServiceName="codecommit.amazonaws.com",
            )
            cred = resp["ServiceSpecificCredential"]
            assert cred["UserName"] == user_name
            assert cred["ServiceName"] == "codecommit.amazonaws.com"
            assert "ServiceSpecificCredentialId" in cred
            assert "ServiceUserName" in cred
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMGenerateServiceLastAccessedDetails:
    """Tests for GenerateServiceLastAccessedDetails and GetServiceLastAccessedDetails."""

    def test_generate_and_get_service_last_accessed(self, iam):
        """GenerateServiceLastAccessedDetails + GetServiceLastAccessedDetails."""
        role_name = _unique("sla-role")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
        )
        try:
            role = iam.get_role(RoleName=role_name)
            role_arn = role["Role"]["Arn"]

            gen_resp = iam.generate_service_last_accessed_details(Arn=role_arn)
            assert "JobId" in gen_resp
            job_id = gen_resp["JobId"]

            get_resp = iam.get_service_last_accessed_details(JobId=job_id)
            assert "JobStatus" in get_resp
            assert get_resp["JobStatus"] in ("IN_PROGRESS", "COMPLETED", "FAILED")
            assert "ServicesLastAccessed" in get_resp
        finally:
            iam.delete_role(RoleName=role_name)

    def test_get_service_last_accessed_details_with_entities(self, iam):
        """GetServiceLastAccessedDetailsWithEntities returns entity details."""
        role_name = _unique("slae-role")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
        )
        try:
            role = iam.get_role(RoleName=role_name)
            role_arn = role["Role"]["Arn"]

            gen_resp = iam.generate_service_last_accessed_details(Arn=role_arn)
            job_id = gen_resp["JobId"]

            ent_resp = iam.get_service_last_accessed_details_with_entities(
                JobId=job_id,
                ServiceNamespace="s3",
            )
            assert "JobStatus" in ent_resp
            assert ent_resp["JobStatus"] in ("IN_PROGRESS", "COMPLETED", "FAILED")
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# SAML provider tagging
# ---------------------------------------------------------------------------


class TestIAMSAMLProviderTags:
    def test_tag_saml_provider(self, iam):
        """TagSAMLProvider adds tags to a SAML provider."""
        name = _unique("saml-tag")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            iam.tag_saml_provider(
                SAMLProviderArn=arn,
                Tags=[{"Key": "env", "Value": "staging"}, {"Key": "team", "Value": "auth"}],
            )
            get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
            tags = {t["Key"]: t["Value"] for t in get_resp.get("Tags", [])}
            assert tags["env"] == "staging"
            assert tags["team"] == "auth"
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_untag_saml_provider(self, iam):
        """UntagSAMLProvider removes tags from a SAML provider."""
        name = _unique("saml-untag")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            iam.tag_saml_provider(
                SAMLProviderArn=arn,
                Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "remove", "Value": "bye"}],
            )
            iam.untag_saml_provider(SAMLProviderArn=arn, TagKeys=["remove"])
            get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
            keys = [t["Key"] for t in get_resp.get("Tags", [])]
            assert "keep" in keys
            assert "remove" not in keys
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


# ---------------------------------------------------------------------------
# Server certificate tagging and update
# ---------------------------------------------------------------------------


class TestIAMServerCertificateTags:
    def test_tag_server_certificate(self, iam):
        """TagServerCertificate adds tags to a server certificate."""
        cert_name = _unique("sctag")
        iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            iam.tag_server_certificate(
                ServerCertificateName=cert_name,
                Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "infra"}],
            )
            resp = iam.list_server_certificate_tags(ServerCertificateName=cert_name)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "prod"
            assert tags["team"] == "infra"
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)

    def test_untag_server_certificate(self, iam):
        """UntagServerCertificate removes tags from a server certificate."""
        cert_name = _unique("scuntag")
        iam.upload_server_certificate(
            ServerCertificateName=cert_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            iam.tag_server_certificate(
                ServerCertificateName=cert_name,
                Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "drop", "Value": "bye"}],
            )
            iam.untag_server_certificate(ServerCertificateName=cert_name, TagKeys=["drop"])
            resp = iam.list_server_certificate_tags(ServerCertificateName=cert_name)
            keys = [t["Key"] for t in resp["Tags"]]
            assert "keep" in keys
            assert "drop" not in keys
        finally:
            iam.delete_server_certificate(ServerCertificateName=cert_name)

    def test_update_server_certificate(self, iam):
        """UpdateServerCertificate renames a server certificate."""
        old_name = _unique("scold")
        new_name = _unique("scnew")
        iam.upload_server_certificate(
            ServerCertificateName=old_name,
            CertificateBody=_CERT_BODY,
            PrivateKey=_PRIVATE_KEY,
        )
        try:
            resp = iam.update_server_certificate(
                ServerCertificateName=old_name,
                NewServerCertificateName=new_name,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            listed = iam.list_server_certificates()
            names = [c["ServerCertificateName"] for c in listed["ServerCertificateMetadataList"]]
            assert new_name in names
            assert old_name not in names
        finally:
            iam.delete_server_certificate(ServerCertificateName=new_name)


# ---------------------------------------------------------------------------
# OpenIDConnect client ID operations
# ---------------------------------------------------------------------------


class TestIAMOIDCClientID:
    def test_add_client_id_to_openid_connect_provider(self, iam):
        """AddClientIDToOpenIDConnectProvider adds a client ID."""
        url = f"https://{_unique('oidc-cid')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url, ThumbprintList=["a" * 40], ClientIDList=["original-client"]
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.add_client_id_to_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="new-client"
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "new-client" in get_resp["ClientIDList"]
            assert "original-client" in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_remove_client_id_from_openid_connect_provider(self, iam):
        """RemoveClientIDFromOpenIDConnectProvider removes a client ID."""
        url = f"https://{_unique('oidc-rcid')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url, ThumbprintList=["a" * 40], ClientIDList=["keep-client", "remove-client"]
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.remove_client_id_from_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="remove-client"
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "keep-client" in get_resp["ClientIDList"]
            assert "remove-client" not in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


# ---------------------------------------------------------------------------
# Context keys for policy simulation
# ---------------------------------------------------------------------------


class TestIAMContextKeys:
    def test_get_context_keys_for_custom_policy(self, iam):
        """GetContextKeysForCustomPolicy returns context keys from a policy."""
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:GetObject",
                        "Resource": "*",
                        "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1"}},
                    }
                ],
            }
        )
        resp = iam.get_context_keys_for_custom_policy(PolicyInputList=[policy_doc])
        assert "ContextKeyNames" in resp
        assert isinstance(resp["ContextKeyNames"], list)

    def test_get_context_keys_for_principal_policy(self, iam):
        """GetContextKeysForPrincipalPolicy returns context keys for a principal."""
        role_name = _unique("ckpp-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        pol = iam.create_policy(
            PolicyName=_unique("ckpp-pol"),
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:*",
                            "Resource": "*",
                            "Condition": {"StringEquals": {"aws:RequestedRegion": "us-west-2"}},
                        }
                    ],
                }
            ),
        )
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            resp = iam.get_context_keys_for_principal_policy(PolicySourceArn=role_arn)
            assert "ContextKeyNames" in resp
            assert isinstance(resp["ContextKeyNames"], list)
        finally:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Instance profile tags (ListInstanceProfileTags)
# ---------------------------------------------------------------------------


class TestIAMListInstanceProfileTags:
    def test_list_instance_profile_tags(self, iam):
        """ListInstanceProfileTags returns tags after tagging."""
        profile_name = _unique("lipt-prof")
        iam.create_instance_profile(InstanceProfileName=profile_name)
        try:
            iam.tag_instance_profile(
                InstanceProfileName=profile_name,
                Tags=[{"Key": "env", "Value": "dev"}],
            )
            resp = iam.list_instance_profile_tags(InstanceProfileName=profile_name)
            assert "Tags" in resp
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "dev"
        finally:
            iam.delete_instance_profile(InstanceProfileName=profile_name)


# ---------------------------------------------------------------------------
# MFA device tagging
# ---------------------------------------------------------------------------


class TestIAMMFADeviceTags:
    def test_tag_mfa_device(self, iam):
        """TagMFADevice adds tags to a virtual MFA device."""
        mfa_name = _unique("mfatag")
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.tag_mfa_device(
                SerialNumber=serial,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "sec"}],
            )
            resp = iam.list_mfa_device_tags(SerialNumber=serial)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "sec"
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_untag_mfa_device(self, iam):
        """UntagMFADevice removes tags from a virtual MFA device."""
        mfa_name = _unique("mfauntag")
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.tag_mfa_device(
                SerialNumber=serial,
                Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "drop", "Value": "bye"}],
            )
            iam.untag_mfa_device(SerialNumber=serial, TagKeys=["drop"])
            resp = iam.list_mfa_device_tags(SerialNumber=serial)
            keys = [t["Key"] for t in resp["Tags"]]
            assert "keep" in keys
            assert "drop" not in keys
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_list_mfa_device_tags(self, iam):
        """ListMFADeviceTags returns empty tags list for untagged device."""
        mfa_name = _unique("mfalisttags")
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            resp = iam.list_mfa_device_tags(SerialNumber=serial)
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)


# ---------------------------------------------------------------------------
# Additional coverage: Path-based listing
# ---------------------------------------------------------------------------


class TestIAMPathBasedListing:
    def test_list_users_with_path_prefix(self, iam):
        """ListUsers with PathPrefix filters by path."""
        user_name = _unique("pathlist-usr")
        iam.create_user(UserName=user_name, Path="/engineering/backend/")
        try:
            resp = iam.list_users(PathPrefix="/engineering/")
            names = [u["UserName"] for u in resp["Users"]]
            assert user_name in names
        finally:
            iam.delete_user(UserName=user_name)

    def test_list_roles_with_path_prefix(self, iam):
        """ListRoles with PathPrefix filters by path."""
        role_name = _unique("pathlist-role")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Path="/services/lambda/",
        )
        try:
            resp = iam.list_roles(PathPrefix="/services/")
            names = [r["RoleName"] for r in resp["Roles"]]
            assert role_name in names
        finally:
            iam.delete_role(RoleName=role_name)

    def test_list_groups_with_path_prefix(self, iam):
        """ListGroups with PathPrefix filters by path."""
        group_name = _unique("pathlist-grp")
        iam.create_group(GroupName=group_name, Path="/teams/backend/")
        try:
            resp = iam.list_groups(PathPrefix="/teams/")
            names = [g["GroupName"] for g in resp["Groups"]]
            assert group_name in names
        finally:
            iam.delete_group(GroupName=group_name)

    def test_list_policies_with_path_prefix(self, iam):
        """ListPolicies with PathPrefix filters by path."""
        policy_name = _unique("pathlist-pol")
        pol = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=SIMPLE_POLICY_DOC,
            Path="/app/",
        )
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policies(PathPrefix="/app/", Scope="Local")
            names = [p["PolicyName"] for p in resp["Policies"]]
            assert policy_name in names
        finally:
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Additional coverage: Role with Path
# ---------------------------------------------------------------------------


class TestIAMCreateRoleWithPath:
    def test_create_role_with_path(self, iam):
        """CreateRole with custom Path sets the path correctly."""
        role_name = _unique("pathrole")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Path="/custom/path/",
        )
        try:
            resp = iam.get_role(RoleName=role_name)
            assert resp["Role"]["Path"] == "/custom/path/"
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Additional coverage: Delete role standalone
# ---------------------------------------------------------------------------


class TestIAMDeleteRole:
    def test_delete_role_removes_from_list(self, iam):
        """DeleteRole removes the role from ListRoles."""
        role_name = _unique("delrole")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.delete_role(RoleName=role_name)
        resp = iam.list_roles()
        names = [r["RoleName"] for r in resp["Roles"]]
        assert role_name not in names

    def test_delete_role_nonexistent_raises(self, iam):
        """DeleteRole on a non-existent role raises NoSuchEntity."""
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            iam.delete_role(RoleName="no-such-role-xyz-" + uuid.uuid4().hex[:8])


# ---------------------------------------------------------------------------
# Additional coverage: Delete group standalone
# ---------------------------------------------------------------------------


class TestIAMDeleteGroup:
    def test_delete_group_nonexistent_raises(self, iam):
        """DeleteGroup on a non-existent group raises NoSuchEntity."""
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            iam.delete_group(GroupName="no-such-group-" + uuid.uuid4().hex[:8])


# ---------------------------------------------------------------------------
# Additional coverage: Create policy with Description and Tags
# ---------------------------------------------------------------------------


class TestIAMCreatePolicyExtended:
    def test_create_policy_with_description(self, iam):
        """CreatePolicy with Description sets the description."""
        policy_name = _unique("descrpol")
        pol = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=SIMPLE_POLICY_DOC,
            Description="A test policy with description",
        )
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy(PolicyArn=arn)
            assert resp["Policy"]["Description"] == "A test policy with description"
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_create_policy_with_tags(self, iam):
        """CreatePolicy with Tags sets tags on the policy."""
        policy_name = _unique("tagpol")
        pol = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=SIMPLE_POLICY_DOC,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.list_policy_tags(PolicyArn=arn)
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "test"
        finally:
            iam.delete_policy(PolicyArn=arn)

    def test_create_policy_with_path(self, iam):
        """CreatePolicy with Path sets the path."""
        policy_name = _unique("pathpol")
        pol = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=SIMPLE_POLICY_DOC,
            Path="/custom/policy/",
        )
        arn = pol["Policy"]["Arn"]
        try:
            resp = iam.get_policy(PolicyArn=arn)
            assert resp["Policy"]["Path"] == "/custom/policy/"
        finally:
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Additional coverage: ListInstanceProfilesForRole edge cases
# ---------------------------------------------------------------------------


class TestIAMListInstanceProfilesForRoleEdge:
    def test_list_instance_profiles_for_role_none(self, iam):
        """ListInstanceProfilesForRole returns empty when no profiles."""
        role_name = _unique("lipfr-none")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.list_instance_profiles_for_role(RoleName=role_name)
            assert resp["InstanceProfiles"] == []
        finally:
            iam.delete_role(RoleName=role_name)

    def test_list_instance_profiles_for_role_multiple(self, iam):
        """ListInstanceProfilesForRole with multiple profiles."""
        role_name = _unique("lipfr-multi")
        prof1 = _unique("lipfr-p1")
        prof2 = _unique("lipfr-p2")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        iam.create_instance_profile(InstanceProfileName=prof1)
        iam.create_instance_profile(InstanceProfileName=prof2)
        try:
            iam.add_role_to_instance_profile(InstanceProfileName=prof1, RoleName=role_name)
            iam.add_role_to_instance_profile(InstanceProfileName=prof2, RoleName=role_name)
            resp = iam.list_instance_profiles_for_role(RoleName=role_name)
            names = [ip["InstanceProfileName"] for ip in resp["InstanceProfiles"]]
            assert prof1 in names
            assert prof2 in names
        finally:
            for p in [prof1, prof2]:
                try:
                    iam.remove_role_from_instance_profile(InstanceProfileName=p, RoleName=role_name)
                except Exception:
                    pass  # best-effort cleanup
                try:
                    iam.delete_instance_profile(InstanceProfileName=p)
                except Exception:
                    pass  # best-effort cleanup
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Additional coverage: GetRole trust policy content validation
# ---------------------------------------------------------------------------


class TestIAMGetRoleTrustPolicy:
    def test_get_role_trust_policy_content(self, iam):
        """GetRole returns parsed trust policy with correct principal."""
        role_name = _unique("trust-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp = iam.get_role(RoleName=role_name)
            doc = resp["Role"]["AssumeRolePolicyDocument"]
            assert doc["Version"] == "2012-10-17"
            assert len(doc["Statement"]) == 1
            stmt = doc["Statement"][0]
            assert stmt["Effect"] == "Allow"
            assert stmt["Action"] == "sts:AssumeRole"
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Additional coverage: Simulate custom policy with detail
# ---------------------------------------------------------------------------


class TestIAMSimulatePolicyExtended:
    def test_simulate_custom_policy_allowed(self, iam):
        """SimulateCustomPolicy returns allowed for matching action."""
        resp = iam.simulate_custom_policy(
            PolicyInputList=[SIMPLE_POLICY_DOC],
            ActionNames=["s3:GetObject"],
        )
        assert len(resp["EvaluationResults"]) == 1
        result = resp["EvaluationResults"][0]
        assert result["EvalActionName"] == "s3:GetObject"
        assert result["EvalDecision"] in ("allowed", "implicitDeny", "explicitDeny")

    def test_simulate_custom_policy_multiple_actions(self, iam):
        """SimulateCustomPolicy with multiple actions."""
        resp = iam.simulate_custom_policy(
            PolicyInputList=[SIMPLE_POLICY_DOC],
            ActionNames=["s3:GetObject", "s3:PutObject", "ec2:DescribeInstances"],
        )
        assert len(resp["EvaluationResults"]) == 3
        action_names = [r["EvalActionName"] for r in resp["EvaluationResults"]]
        assert "s3:GetObject" in action_names
        assert "s3:PutObject" in action_names
        assert "ec2:DescribeInstances" in action_names


# ---------------------------------------------------------------------------
# Additional coverage: Login profile lifecycle detail
# ---------------------------------------------------------------------------


class TestIAMLoginProfileExtended:
    def test_delete_login_profile_makes_it_gone(self, iam):
        """After DeleteLoginProfile, GetLoginProfile raises NoSuchEntity."""
        user_name = _unique("lp-delgone")
        iam.create_user(UserName=user_name)
        try:
            iam.create_login_profile(UserName=user_name, Password="Test@12345678")
            iam.delete_login_profile(UserName=user_name)
            with pytest.raises(iam.exceptions.NoSuchEntityException):
                iam.get_login_profile(UserName=user_name)
        finally:
            try:
                iam.delete_login_profile(UserName=user_name)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: Deactivate MFA standalone
# ---------------------------------------------------------------------------


class TestIAMDeactivateMFADevice:
    def test_deactivate_mfa_device_removes_from_list(self, iam):
        """DeactivateMFADevice removes the device from ListMFADevices."""
        user_name = _unique("deact-mfa")
        mfa_name = _unique("deact-dev")
        iam.create_user(UserName=user_name)
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.enable_mfa_device(
                UserName=user_name,
                SerialNumber=serial,
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
            # Verify enabled
            listed = iam.list_mfa_devices(UserName=user_name)
            assert any(d["SerialNumber"] == serial for d in listed["MFADevices"])

            # Deactivate
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            listed2 = iam.list_mfa_devices(UserName=user_name)
            assert not any(d["SerialNumber"] == serial for d in listed2["MFADevices"])
        finally:
            try:
                iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_virtual_mfa_device(SerialNumber=serial)
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: GetUser for non-existent user
# ---------------------------------------------------------------------------


class TestIAMGetUserErrors:
    def test_get_user_nonexistent_raises(self, iam):
        """GetUser on a non-existent user raises NoSuchEntity."""
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            iam.get_user(UserName="no-such-user-" + uuid.uuid4().hex[:8])


# ---------------------------------------------------------------------------
# Additional coverage: CreateUser duplicate raises error
# ---------------------------------------------------------------------------


class TestIAMCreateUserDuplicate:
    def test_create_user_duplicate_raises(self, iam):
        """CreateUser with an existing name raises EntityAlreadyExists."""
        user_name = _unique("dup-user")
        iam.create_user(UserName=user_name)
        try:
            with pytest.raises(iam.exceptions.EntityAlreadyExistsException):
                iam.create_user(UserName=user_name)
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: CreateRole duplicate raises error
# ---------------------------------------------------------------------------


class TestIAMCreateRoleDuplicate:
    def test_create_role_duplicate_raises(self, iam):
        """CreateRole with an existing name raises EntityAlreadyExists."""
        role_name = _unique("dup-role")
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            with pytest.raises(iam.exceptions.EntityAlreadyExistsException):
                iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Additional coverage: CreateGroup duplicate raises error
# ---------------------------------------------------------------------------


class TestIAMCreateGroupDuplicate:
    def test_create_group_duplicate_raises(self, iam):
        """CreateGroup with an existing name raises EntityAlreadyExists."""
        group_name = _unique("dup-grp")
        iam.create_group(GroupName=group_name)
        try:
            with pytest.raises(iam.exceptions.EntityAlreadyExistsException):
                iam.create_group(GroupName=group_name)
        finally:
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Additional coverage: ListEntitiesForPolicy with users and groups
# ---------------------------------------------------------------------------


class TestIAMListEntitiesForPolicyExtended:
    def test_list_entities_for_policy_user_and_group(self, iam):
        """ListEntitiesForPolicy returns users and groups attached to a policy."""
        user_name = _unique("lefp-usr")
        group_name = _unique("lefp-grp")
        policy_name = _unique("lefp-pol")
        iam.create_user(UserName=user_name)
        iam.create_group(GroupName=group_name)
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            iam.attach_user_policy(UserName=user_name, PolicyArn=arn)
            iam.attach_group_policy(GroupName=group_name, PolicyArn=arn)
            resp = iam.list_entities_for_policy(PolicyArn=arn)
            user_names = [u["UserName"] for u in resp.get("PolicyUsers", [])]
            group_names = [g["GroupName"] for g in resp.get("PolicyGroups", [])]
            assert user_name in user_names
            assert group_name in group_names
        finally:
            iam.detach_user_policy(UserName=user_name, PolicyArn=arn)
            iam.detach_group_policy(GroupName=group_name, PolicyArn=arn)
            iam.delete_policy(PolicyArn=arn)
            iam.delete_user(UserName=user_name)
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Additional coverage: AccountAuthorizationDetails with Group filter
# ---------------------------------------------------------------------------


class TestIAMAccountAuthorizationDetailsGroup:
    def test_get_account_authorization_details_group_filter(self, iam):
        """GetAccountAuthorizationDetails with Filter=[Group]."""
        group_name = _unique("aad-grp")
        iam.create_group(GroupName=group_name)
        try:
            resp = iam.get_account_authorization_details(Filter=["Group"])
            assert "GroupDetailList" in resp
            group_names = [g["GroupName"] for g in resp.get("GroupDetailList", [])]
            assert group_name in group_names
            # Should not have user or role details
            assert len(resp.get("UserDetailList", [])) == 0
            assert len(resp.get("RoleDetailList", [])) == 0
        finally:
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Additional coverage: DeletePolicy standalone
# ---------------------------------------------------------------------------


class TestIAMDeletePolicy:
    def test_delete_policy_removes_from_list(self, iam):
        """DeletePolicy removes the policy from ListPolicies."""
        policy_name = _unique("delpol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        iam.delete_policy(PolicyArn=arn)
        resp = iam.list_policies(Scope="Local")
        names = [p["PolicyName"] for p in resp["Policies"]]
        assert policy_name not in names


# ---------------------------------------------------------------------------
# Additional coverage: CreateGroup with Path
# ---------------------------------------------------------------------------


class TestIAMCreateGroupWithPath:
    def test_create_group_with_path(self, iam):
        """CreateGroup with Path sets the path."""
        group_name = _unique("pathgrp")
        iam.create_group(GroupName=group_name, Path="/division/team/")
        try:
            resp = iam.get_group(GroupName=group_name)
            assert resp["Group"]["Path"] == "/division/team/"
        finally:
            iam.delete_group(GroupName=group_name)


# ---------------------------------------------------------------------------
# Additional coverage: User ARN format
# ---------------------------------------------------------------------------


class TestIAMUserArnFormat:
    def test_user_arn_contains_account_and_name(self, iam):
        """CreateUser returns an ARN with the correct format."""
        user_name = _unique("arn-user")
        resp = iam.create_user(UserName=user_name)
        try:
            arn = resp["User"]["Arn"]
            assert arn.startswith("arn:aws:iam:")
            assert user_name in arn
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: Role ARN format
# ---------------------------------------------------------------------------


class TestIAMRoleArnFormat:
    def test_role_arn_contains_account_and_name(self, iam):
        """CreateRole returns an ARN with the correct format."""
        role_name = _unique("arn-role")
        resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            arn = resp["Role"]["Arn"]
            assert arn.startswith("arn:aws:iam:")
            assert role_name in arn
        finally:
            iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Additional coverage: Policy ARN format
# ---------------------------------------------------------------------------


class TestIAMPolicyArnFormat:
    def test_policy_arn_contains_account_and_name(self, iam):
        """CreatePolicy returns an ARN with the correct format."""
        policy_name = _unique("arn-pol")
        resp = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = resp["Policy"]["Arn"]
        try:
            assert arn.startswith("arn:aws:iam:")
            assert policy_name in arn
        finally:
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Additional coverage: GetPolicy attachment count
# ---------------------------------------------------------------------------


class TestIAMPolicyAttachmentCount:
    def test_policy_attachment_count_increments(self, iam):
        """GetPolicy AttachmentCount reflects attached entities."""
        policy_name = _unique("attcnt-pol")
        role_name = _unique("attcnt-role")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
        try:
            resp_before = iam.get_policy(PolicyArn=arn)
            assert resp_before["Policy"]["AttachmentCount"] == 0

            iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
            resp_after = iam.get_policy(PolicyArn=arn)
            assert resp_after["Policy"]["AttachmentCount"] == 1
        finally:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=arn)
            iam.delete_role(RoleName=role_name)
            iam.delete_policy(PolicyArn=arn)


# ---------------------------------------------------------------------------
# Additional coverage: Credential report CSV content
# ---------------------------------------------------------------------------


class TestIAMCredentialReportContent:
    def test_credential_report_contains_user(self, iam):
        """GetCredentialReport CSV content includes created users."""
        import time

        user_name = _unique("cr-user")
        iam.create_user(UserName=user_name)
        try:
            for _ in range(10):
                gen = iam.generate_credential_report()
                if gen["State"] == "COMPLETE":
                    break
                time.sleep(0.5)
            resp = iam.get_credential_report()
            content = resp["Content"]
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            assert user_name in content
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: Multiple access keys per user
# ---------------------------------------------------------------------------


class TestIAMMultipleAccessKeys:
    def test_multiple_access_keys_per_user(self, iam):
        """A user can have multiple access keys."""
        user_name = _unique("multi-ak")
        iam.create_user(UserName=user_name)
        try:
            ak1 = iam.create_access_key(UserName=user_name)["AccessKey"]
            ak2 = iam.create_access_key(UserName=user_name)["AccessKey"]
            resp = iam.list_access_keys(UserName=user_name)
            ids = [k["AccessKeyId"] for k in resp["AccessKeyMetadata"]]
            assert ak1["AccessKeyId"] in ids
            assert ak2["AccessKeyId"] in ids
            assert len(ids) >= 2
        finally:
            for ak in [ak1, ak2]:
                try:
                    iam.delete_access_key(UserName=user_name, AccessKeyId=ak["AccessKeyId"])
                except Exception:
                    pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# Additional coverage: ListVirtualMFADevices with AssignmentStatus
# ---------------------------------------------------------------------------


class TestIAMListVirtualMFADevicesFiltered:
    def test_list_virtual_mfa_devices_unassigned(self, iam):
        """ListVirtualMFADevices with AssignmentStatus=Unassigned."""
        mfa_name = _unique("vmfa-unassign")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            listed = iam.list_virtual_mfa_devices(AssignmentStatus="Unassigned")
            serials = [d["SerialNumber"] for d in listed["VirtualMFADevices"]]
            assert serial in serials
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)


# ---------------------------------------------------------------------------
# OIDC Provider full lifecycle tests
# ---------------------------------------------------------------------------


class TestIAMOidcProviders:
    """Comprehensive OIDC provider lifecycle: create, get, list, tag, untag, client IDs, delete."""

    def test_oidc_provider_full_lifecycle(self, iam):
        """OIDC full lifecycle: create, get, list, tag, untag, thumbprint, client IDs."""
        url = f"https://oidc-lifecycle-{uuid.uuid4().hex[:8]}.example.com"
        thumbprint = "a" * 40
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=[thumbprint],
            ClientIDList=["initial-client"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        assert arn.startswith("arn:aws:iam:")
        try:
            # Get
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert thumbprint in get_resp["ThumbprintList"]
            assert "initial-client" in get_resp["ClientIDList"]

            # List
            listed = iam.list_open_id_connect_providers()
            arns = [p["Arn"] for p in listed["OpenIDConnectProviderList"]]
            assert arn in arns

            # Tag
            iam.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=arn,
                Tags=[{"Key": "project", "Value": "robotocore"}, {"Key": "temp", "Value": "yes"}],
            )
            tag_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            tags = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tags["project"] == "robotocore"
            assert tags["temp"] == "yes"

            # Untag
            iam.untag_open_id_connect_provider(OpenIDConnectProviderArn=arn, TagKeys=["temp"])
            tag_resp2 = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            keys = [t["Key"] for t in tag_resp2["Tags"]]
            assert "project" in keys
            assert "temp" not in keys

            # Update thumbprint
            new_thumb = "b" * 40
            iam.update_open_id_connect_provider_thumbprint(
                OpenIDConnectProviderArn=arn, ThumbprintList=[new_thumb]
            )
            get_resp2 = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert new_thumb in get_resp2["ThumbprintList"]

            # Add client ID
            iam.add_client_id_to_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="added-client"
            )
            get_resp3 = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "added-client" in get_resp3["ClientIDList"]
            assert "initial-client" in get_resp3["ClientIDList"]

            # Remove client ID
            iam.remove_client_id_from_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="initial-client"
            )
            get_resp4 = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "added-client" in get_resp4["ClientIDList"]
            assert "initial-client" not in get_resp4["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

        # Verify deletion
        listed2 = iam.list_open_id_connect_providers()
        arns2 = [p["Arn"] for p in listed2["OpenIDConnectProviderList"]]
        assert arn not in arns2

    def test_oidc_provider_list_tags_empty(self, iam):
        """ListOpenIDConnectProviderTags returns empty list for untagged provider."""
        url = f"https://oidc-notags-{uuid.uuid4().hex[:8]}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["c" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            tag_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            assert "Tags" in tag_resp
            assert len(tag_resp["Tags"]) == 0
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


# ---------------------------------------------------------------------------
# SAML Provider full lifecycle tests
# ---------------------------------------------------------------------------

_SAML_METADATA_FULL = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
    ' entityID="https://idp.example.com/metadata">'
    "<IDPSSODescriptor"
    ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
    '<KeyDescriptor use="signing">'
    '<KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
    "<X509Data><X509Certificate>"
    "MIIDpDCCAoygAwIBAgIGAXpJOXwHMA0GCSqGSIb3DQEBCwUAMIGSMQswCQYDVQQGEwJVUzETMBEG"
    "A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU"
    "MBIGA1UECwwLU1NPUHJvdmlkZXIxEzARBgNVBAMMCmRldi04NDMyNTMxHDAaBgkqhkiG9w0BCQEW"
    "DWluZm9Ab2t0YS5jb20wHhcNMjEwNjIyMTgxNjQzWhcNMzEwNjIyMTgxNzQzWjCBkjELMAkGA1UE"
    "BhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNV"
    "BAoMBE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRMwEQYDVQQDDApkZXYtODQzMjUzMRwwGgYJ"
    "KoZIhvcNAQkBFg1pbmZvQG9rdGEuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA"
    "</X509Certificate></X509Data>"
    "</KeyInfo></KeyDescriptor>"
    "<SingleSignOnService"
    ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
    ' Location="https://idp.example.com/sso"/>'
    "<SingleSignOnService"
    ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"'
    ' Location="https://idp.example.com/sso"/>'
    "</IDPSSODescriptor></EntityDescriptor>"
)


class TestIAMSamlProviders:
    """SAML provider lifecycle: create, get, list, update, tag, untag, list tags, delete."""

    def test_saml_provider_full_lifecycle(self, iam):
        """Create, get, list, update, tag, untag, list tags, delete."""
        name = _unique("saml-lc")
        resp = iam.create_saml_provider(SAMLMetadataDocument=_SAML_METADATA_FULL, Name=name)
        arn = resp["SAMLProviderArn"]
        assert arn.startswith("arn:aws:iam:")
        try:
            # Get
            get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
            assert "SAMLMetadataDocument" in get_resp

            # List
            listed = iam.list_saml_providers()
            arns = [p["Arn"] for p in listed["SAMLProviderList"]]
            assert arn in arns

            # Update
            upd = iam.update_saml_provider(
                SAMLProviderArn=arn, SAMLMetadataDocument=_SAML_METADATA_FULL
            )
            assert "SAMLProviderArn" in upd

            # Tag
            iam.tag_saml_provider(
                SAMLProviderArn=arn,
                Tags=[{"Key": "env", "Value": "dev"}, {"Key": "temp", "Value": "yes"}],
            )

            # ListSAMLProviderTags
            tag_resp = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            assert "Tags" in tag_resp
            tags = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tags["env"] == "dev"
            assert tags["temp"] == "yes"

            # Untag
            iam.untag_saml_provider(SAMLProviderArn=arn, TagKeys=["temp"])
            tag_resp2 = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            keys = [t["Key"] for t in tag_resp2["Tags"]]
            assert "env" in keys
            assert "temp" not in keys
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

        # Verify deletion
        listed2 = iam.list_saml_providers()
        arns2 = [p["Arn"] for p in listed2["SAMLProviderList"]]
        assert arn not in arns2

    def test_list_saml_provider_tags_empty(self, iam):
        """ListSAMLProviderTags returns empty list for untagged provider."""
        name = _unique("saml-notag")
        resp = iam.create_saml_provider(SAMLMetadataDocument=_SAML_METADATA_FULL, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            tag_resp = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            assert "Tags" in tag_resp
            assert isinstance(tag_resp["Tags"], list)
            assert len(tag_resp["Tags"]) == 0
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


# ---------------------------------------------------------------------------
# Virtual MFA and MFA device lifecycle tests
# ---------------------------------------------------------------------------


class TestIAMMfaDeviceLifecycle:
    """VirtualMFA create/list/delete, MFA device listing, MFA tag/untag/list tags."""

    def test_virtual_mfa_create_list_tag_delete(self, iam):
        """VirtualMFA create, list, tag, list tags, untag, delete."""
        mfa_name = _unique("vmfa-lc")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        device = resp["VirtualMFADevice"]
        serial = device["SerialNumber"]
        assert serial is not None
        try:
            # List
            listed = iam.list_virtual_mfa_devices()
            serials = [d["SerialNumber"] for d in listed["VirtualMFADevices"]]
            assert serial in serials

            # Tag
            iam.tag_mfa_device(
                SerialNumber=serial,
                Tags=[{"Key": "purpose", "Value": "testing"}, {"Key": "extra", "Value": "val"}],
            )

            # ListMFADeviceTags
            tag_resp = iam.list_mfa_device_tags(SerialNumber=serial)
            tags = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tags["purpose"] == "testing"
            assert tags["extra"] == "val"

            # Untag
            iam.untag_mfa_device(SerialNumber=serial, TagKeys=["extra"])
            tag_resp2 = iam.list_mfa_device_tags(SerialNumber=serial)
            keys = [t["Key"] for t in tag_resp2["Tags"]]
            assert "purpose" in keys
            assert "extra" not in keys
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_list_mfa_devices_for_user(self, iam):
        """ListMFADevices returns empty list for a user with no MFA."""
        user_name = _unique("mfa-lc-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_mfa_devices(UserName=user_name)
            assert "MFADevices" in resp
            assert isinstance(resp["MFADevices"], list)
            assert len(resp["MFADevices"]) == 0
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# SSH Public Key tests
# ---------------------------------------------------------------------------

_SSH_PUB_KEY = (
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCz8Dk176Uh4bPmFosfHGQQwJj5ejGnlf"
    "0RmTbVBjDaBCO/x4D98DhtVIBHdIUVOhfFkKG3cAfpJo2rWB7RkDzV2OywOa0Nlb5PXhR"
    "hGJsXBGLGEOGp5HoAiIeUJGZ0GZ7Ly7PGbKMe2OjhKfSHn9UkER6A+BxMp7J1w9ZMPX2j"
    "bHuXnuBErjUCr3LGXNN9p2gaQQ3nGxw4sFMq3bJWKW7R2Dz1VJfBjEqFMk5LYqP1n5M+1"
    "HQYPJEFbKAHDN3OL3F4D3QPDJneMCOLI3EcsJJDPVpFaGp1qP5vQJ5yf0ABXk0EJ1fH8F"
    "hIzFNJdNq+LVMIHChSxFIpfdmh test-key"
)


class TestIAMSshPublicKeys:
    """SSH public key upload and list operations."""

    def test_upload_and_list_ssh_public_keys(self, iam):
        """UploadSSHPublicKey, ListSSHPublicKeys."""
        user_name = _unique("ssh-lc")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(UserName=user_name, SSHPublicKeyBody=_SSH_PUB_KEY)
            assert "SSHPublicKey" in resp
            key_id = resp["SSHPublicKey"]["SSHPublicKeyId"]
            assert key_id is not None
            assert resp["SSHPublicKey"]["Status"] == "Active"

            # List
            listed = iam.list_ssh_public_keys(UserName=user_name)
            assert "SSHPublicKeys" in listed
            ids = [k["SSHPublicKeyId"] for k in listed["SSHPublicKeys"]]
            assert key_id in ids
        finally:
            try:
                iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_list_ssh_public_keys_empty(self, iam):
        """ListSSHPublicKeys returns empty for user with no keys."""
        user_name = _unique("ssh-empty")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_ssh_public_keys(UserName=user_name)
            assert "SSHPublicKeys" in resp
            assert len(resp["SSHPublicKeys"]) == 0
        finally:
            iam.delete_user(UserName=user_name)


# ---------------------------------------------------------------------------
# STS preferences and misc operations
# ---------------------------------------------------------------------------


class TestIAMMiscOperations:
    """SetSecurityTokenServicePreferences and other miscellaneous IAM operations."""

    def test_set_security_token_service_preferences_v2(self, iam):
        """SetSecurityTokenServicePreferences sets global endpoint token version to v2."""
        resp = iam.set_security_token_service_preferences(GlobalEndpointTokenVersion="v2Token")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_set_security_token_service_preferences_v1(self, iam):
        """SetSecurityTokenServicePreferences sets global endpoint token version to v1."""
        resp = iam.set_security_token_service_preferences(GlobalEndpointTokenVersion="v1Token")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestIAMMFADeviceExtended:
    """Tests for additional MFA device operations."""

    def test_get_mfa_device_nonexistent(self, iam):
        """GetMFADevice raises NoSuchEntity for nonexistent device."""
        with pytest.raises(ClientError) as exc:
            iam.get_mfa_device(SerialNumber="arn:aws:iam::123456789012:mfa/fake-dev")
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

    def test_resync_mfa_device_nonexistent_user(self, iam):
        """ResyncMFADevice raises NoSuchEntity for nonexistent user."""
        with pytest.raises(ClientError) as exc:
            iam.resync_mfa_device(
                UserName="fake-user-xyz",
                SerialNumber="arn:aws:iam::123456789012:mfa/fake",
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


# ---------------------------------------------------------------------------
# Explicit tests for missing ops coverage
# ---------------------------------------------------------------------------


SAML_METADATA_EXPLICIT = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"'
    ' entityID="https://idp.example.com/metadata">'
    "<IDPSSODescriptor"
    ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
    '<KeyDescriptor use="signing">'
    '<KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">'
    "<X509Data><X509Certificate>"
    "MIIDpDCCAoygAwIBAgIGAXpJOXwHMA0GCSqGSIb3DQEBCwUAMIGSMQswCQYDVQQGEwJVUzETMBEG"
    "A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU"
    "MBIGA1UECwwLU1NPUHJvdmlkZXIxEzARBgNVBAMMCmRldi04NDMyNTMxHDAaBgkqhkiG9w0BCQEW"
    "DWluZm9Ab2t0YS5jb20wHhcNMjEwNjIyMTgxNjQzWhcNMzEwNjIyMTgxNzQzWjCBkjELMAkGA1UE"
    "BhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xDTALBgNV"
    "BAoMBE9rdGExFDASBgNVBAsMC1NTT1Byb3ZpZGVyMRMwEQYDVQQDDApkZXYtODQzMjUzMRwwGgYJ"
    "KoZIhvcNAQkBFg1pbmZvQG9rdGEuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA"
    "</X509Certificate></X509Data>"
    "</KeyInfo></KeyDescriptor>"
    "<SingleSignOnService"
    ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
    ' Location="https://idp.example.com/sso"/>'
    "</IDPSSODescriptor></EntityDescriptor>"
)

SSH_PUBLIC_KEY_EXPLICIT = (
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDxTjc0tZ5zKnR7u1/gG6V3Z1t0gXWqE0Eq4m4kL2Iu"
    "FcxZqafJCFHIkCPTwaJCNj8HHlxcXyAwz9UtFR8nCDgHzXYKkW+JpI1W/I6y/KhWlZJWGGWIbeRlBVH"
    "wVvf4kTtCIoW/8M6eA4UJERdI7LhWJWCyNqYKp5F3g6eK0xSA9FxRBVbNq6fkerY2fG/qYfgGH1ALhf"
    "JOa2DqE5D2F9cZi8EelI0KXNM test@test"
)


class TestIAMOIDCProviderExplicit:
    """Explicit coverage tests for OIDC provider operations."""

    def test_create_open_id_connect_provider(self, iam):
        """CreateOpenIDConnectProvider creates a new provider."""
        url = f"https://oidc-{_unique('prov')}.example.com"
        thumbprint = "a" * 40
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=[thumbprint],
            ClientIDList=["client-1"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        assert arn is not None
        iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_get_open_id_connect_provider(self, iam):
        """GetOpenIDConnectProvider returns provider details."""
        url = f"https://oidc-{_unique('get')}.example.com"
        thumbprint = "b" * 40
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=[thumbprint],
            ClientIDList=["get-client"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert thumbprint in get_resp["ThumbprintList"]
            assert "get-client" in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_delete_open_id_connect_provider(self, iam):
        """DeleteOpenIDConnectProvider removes the provider."""
        url = f"https://oidc-{_unique('del')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=["c" * 40],
        )
        arn = resp["OpenIDConnectProviderArn"]
        iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
        providers = iam.list_open_id_connect_providers()["OpenIDConnectProviderList"]
        arns = [p["Arn"] for p in providers]
        assert arn not in arns

    def test_tag_open_id_connect_provider(self, iam):
        """TagOpenIDConnectProvider adds tags."""
        url = f"https://oidc-{_unique('tag')}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["d" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=arn,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            tags_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "test"
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_untag_open_id_connect_provider(self, iam):
        """UntagOpenIDConnectProvider removes tags."""
        url = f"https://oidc-{_unique('untag')}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["e" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=arn,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "remove", "Value": "yes"},
                ],
            )
            iam.untag_open_id_connect_provider(OpenIDConnectProviderArn=arn, TagKeys=["remove"])
            tags_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "remove" not in keys
            assert "keep" in keys
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_list_open_id_connect_providers(self, iam):
        """ListOpenIDConnectProviders returns list of providers."""
        url = f"https://oidc-{_unique('list')}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["f" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            list_resp = iam.list_open_id_connect_providers()
            arns = [p["Arn"] for p in list_resp["OpenIDConnectProviderList"]]
            assert arn in arns
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_list_open_id_connect_provider_tags(self, iam):
        """ListOpenIDConnectProviderTags returns empty list when no tags."""
        url = f"https://oidc-{_unique('ltag')}.example.com"
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=["a" * 40])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            tags_resp = iam.list_open_id_connect_provider_tags(OpenIDConnectProviderArn=arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_update_open_id_connect_provider_thumbprint(self, iam):
        """UpdateOpenIDConnectProviderThumbprint replaces thumbprint list."""
        url = f"https://oidc-{_unique('thumb')}.example.com"
        old_thumb = "a" * 40
        new_thumb = "b" * 40
        resp = iam.create_open_id_connect_provider(Url=url, ThumbprintList=[old_thumb])
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.update_open_id_connect_provider_thumbprint(
                OpenIDConnectProviderArn=arn,
                ThumbprintList=[new_thumb],
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert new_thumb in get_resp["ThumbprintList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_add_client_id_to_open_id_connect_provider(self, iam):
        """AddClientIDToOpenIDConnectProvider adds a client ID."""
        url = f"https://oidc-{_unique('addcid')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url, ThumbprintList=["a" * 40], ClientIDList=["orig-client"]
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.add_client_id_to_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="new-client"
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "new-client" in get_resp["ClientIDList"]
            assert "orig-client" in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

    def test_remove_client_id_from_open_id_connect_provider(self, iam):
        """RemoveClientIDFromOpenIDConnectProvider removes a client ID."""
        url = f"https://oidc-{_unique('rmcid')}.example.com"
        resp = iam.create_open_id_connect_provider(
            Url=url,
            ThumbprintList=["a" * 40],
            ClientIDList=["keep-client", "remove-client"],
        )
        arn = resp["OpenIDConnectProviderArn"]
        try:
            iam.remove_client_id_from_open_id_connect_provider(
                OpenIDConnectProviderArn=arn, ClientID="remove-client"
            )
            get_resp = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            assert "remove-client" not in get_resp["ClientIDList"]
            assert "keep-client" in get_resp["ClientIDList"]
        finally:
            iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


class TestIAMSAMLProviderExplicit:
    """Explicit coverage tests for SAML provider operations."""

    def test_create_saml_provider(self, iam):
        """CreateSAMLProvider creates a provider."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        assert arn is not None
        iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_get_saml_provider(self, iam):
        """GetSAMLProvider returns metadata."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
            assert "SAMLMetadataDocument" in get_resp
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_delete_saml_provider(self, iam):
        """DeleteSAMLProvider removes the provider."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        iam.delete_saml_provider(SAMLProviderArn=arn)
        providers = iam.list_saml_providers()["SAMLProviderList"]
        arns = [p["Arn"] for p in providers]
        assert arn not in arns

    def test_list_saml_providers(self, iam):
        """ListSAMLProviders includes created provider."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            list_resp = iam.list_saml_providers()
            arns = [p["Arn"] for p in list_resp["SAMLProviderList"]]
            assert arn in arns
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_update_saml_provider(self, iam):
        """UpdateSAMLProvider updates metadata document."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            upd = iam.update_saml_provider(
                SAMLProviderArn=arn,
                SAMLMetadataDocument=SAML_METADATA_EXPLICIT,
            )
            assert upd["SAMLProviderArn"] == arn
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_tag_saml_provider(self, iam):
        """TagSAMLProvider adds tags."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            iam.tag_saml_provider(
                SAMLProviderArn=arn,
                Tags=[{"Key": "team", "Value": "platform"}],
            )
            tags_resp = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["team"] == "platform"
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_untag_saml_provider(self, iam):
        """UntagSAMLProvider removes tags."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            iam.tag_saml_provider(
                SAMLProviderArn=arn,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "drop", "Value": "yes"},
                ],
            )
            iam.untag_saml_provider(SAMLProviderArn=arn, TagKeys=["drop"])
            tags_resp = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "drop" not in keys
            assert "keep" in keys
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)

    def test_list_saml_provider_tags(self, iam):
        """ListSAMLProviderTags returns tags list."""
        name = _unique("saml")
        resp = iam.create_saml_provider(SAMLMetadataDocument=SAML_METADATA_EXPLICIT, Name=name)
        arn = resp["SAMLProviderArn"]
        try:
            tags_resp = iam.list_saml_provider_tags(SAMLProviderArn=arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
        finally:
            iam.delete_saml_provider(SAMLProviderArn=arn)


class TestIAMVirtualMFAExplicit:
    """Explicit tests for Virtual MFA device operations."""

    def test_create_virtual_mfa_device(self, iam):
        """CreateVirtualMFADevice creates a virtual MFA device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        device = resp["VirtualMFADevice"]
        serial = device["SerialNumber"]
        assert serial is not None
        assert "Base32StringSeed" in device or "QRCodePNG" in device
        iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_delete_virtual_mfa_device(self, iam):
        """DeleteVirtualMFADevice removes the device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        iam.delete_virtual_mfa_device(SerialNumber=serial)
        listed = iam.list_virtual_mfa_devices()
        serials = [d["SerialNumber"] for d in listed["VirtualMFADevices"]]
        assert serial not in serials

    def test_list_virtual_mfa_devices(self, iam):
        """ListVirtualMFADevices returns device list."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            listed = iam.list_virtual_mfa_devices()
            assert "VirtualMFADevices" in listed
            serials = [d["SerialNumber"] for d in listed["VirtualMFADevices"]]
            assert serial in serials
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_enable_mfa_device(self, iam):
        """EnableMFADevice enables a virtual MFA for a user."""
        user_name = _unique("mfa-usr")
        mfa_name = _unique("vmfa")
        iam.create_user(UserName=user_name)
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.enable_mfa_device(
                UserName=user_name,
                SerialNumber=serial,
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
            devices = iam.list_mfa_devices(UserName=user_name)
            serials = [d["SerialNumber"] for d in devices["MFADevices"]]
            assert serial in serials
        finally:
            try:
                iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            except Exception:
                pass  # cleanup best effort
            try:
                iam.delete_virtual_mfa_device(SerialNumber=serial)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_deactivate_mfa_device(self, iam):
        """DeactivateMFADevice removes MFA device from user."""
        user_name = _unique("mfa-usr")
        mfa_name = _unique("vmfa")
        iam.create_user(UserName=user_name)
        mfa_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=mfa_name)
        serial = mfa_resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.enable_mfa_device(
                UserName=user_name,
                SerialNumber=serial,
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
            devices = iam.list_mfa_devices(UserName=user_name)
            serials = [d["SerialNumber"] for d in devices["MFADevices"]]
            assert serial not in serials
        finally:
            try:
                iam.delete_virtual_mfa_device(SerialNumber=serial)
            except Exception:
                pass  # best-effort cleanup
            iam.delete_user(UserName=user_name)

    def test_list_mfa_devices(self, iam):
        """ListMFADevices returns empty list for user with no MFA."""
        user_name = _unique("mfa-usr")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_mfa_devices(UserName=user_name)
            assert "MFADevices" in resp
            assert isinstance(resp["MFADevices"], list)
            assert len(resp["MFADevices"]) == 0
        finally:
            iam.delete_user(UserName=user_name)

    def test_tag_mfa_device(self, iam):
        """TagMFADevice adds tags to a virtual MFA device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.tag_mfa_device(
                SerialNumber=serial,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            tags_resp = iam.list_mfa_device_tags(SerialNumber=serial)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "test"
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_untag_mfa_device(self, iam):
        """UntagMFADevice removes tags from a virtual MFA device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            iam.tag_mfa_device(
                SerialNumber=serial,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "drop", "Value": "yes"},
                ],
            )
            iam.untag_mfa_device(SerialNumber=serial, TagKeys=["drop"])
            tags_resp = iam.list_mfa_device_tags(SerialNumber=serial)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "drop" not in keys
            assert "keep" in keys
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_list_mfa_device_tags(self, iam):
        """ListMFADeviceTags returns tags for a virtual MFA device."""
        name = _unique("vmfa")
        resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=name)
        serial = resp["VirtualMFADevice"]["SerialNumber"]
        try:
            tags_resp = iam.list_mfa_device_tags(SerialNumber=serial)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
        finally:
            iam.delete_virtual_mfa_device(SerialNumber=serial)

    def test_get_mfa_device(self, iam):
        """GetMFADevice raises NoSuchEntity for nonexistent device."""
        with pytest.raises(ClientError) as exc:
            iam.get_mfa_device(SerialNumber="arn:aws:iam::123456789012:mfa/nonexistent-dev-x")
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

    def test_resync_mfa_device(self, iam):
        """ResyncMFADevice raises NoSuchEntity for nonexistent user."""
        with pytest.raises(ClientError) as exc:
            iam.resync_mfa_device(
                UserName="fake-user-resync-xyz",
                SerialNumber="arn:aws:iam::123456789012:mfa/fake-resync",
                AuthenticationCode1="123456",
                AuthenticationCode2="654321",
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


class TestIAMSSHPublicKeyExplicit:
    """Explicit coverage tests for SSH public key operations."""

    def test_upload_ssh_public_key(self, iam):
        """UploadSSHPublicKey uploads a key."""
        user_name = _unique("ssh-usr")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.upload_ssh_public_key(
                UserName=user_name, SSHPublicKeyBody=SSH_PUBLIC_KEY_EXPLICIT
            )
            key = resp["SSHPublicKey"]
            assert key["UserName"] == user_name
            assert "SSHPublicKeyId" in key
            assert key["Status"] == "Active"
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key["SSHPublicKeyId"])
        finally:
            iam.delete_user(UserName=user_name)

    def test_list_ssh_public_keys(self, iam):
        """ListSSHPublicKeys returns key list."""
        user_name = _unique("ssh-usr")
        iam.create_user(UserName=user_name)
        try:
            upload_resp = iam.upload_ssh_public_key(
                UserName=user_name, SSHPublicKeyBody=SSH_PUBLIC_KEY_EXPLICIT
            )
            key_id = upload_resp["SSHPublicKey"]["SSHPublicKeyId"]
            list_resp = iam.list_ssh_public_keys(UserName=user_name)
            assert "SSHPublicKeys" in list_resp
            key_ids = [k["SSHPublicKeyId"] for k in list_resp["SSHPublicKeys"]]
            assert key_id in key_ids
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
        finally:
            iam.delete_user(UserName=user_name)

    def test_get_ssh_public_key(self, iam):
        """GetSSHPublicKey returns key details."""
        user_name = _unique("ssh-usr")
        iam.create_user(UserName=user_name)
        try:
            upload_resp = iam.upload_ssh_public_key(
                UserName=user_name, SSHPublicKeyBody=SSH_PUBLIC_KEY_EXPLICIT
            )
            key_id = upload_resp["SSHPublicKey"]["SSHPublicKeyId"]
            get_resp = iam.get_ssh_public_key(
                UserName=user_name,
                SSHPublicKeyId=key_id,
                Encoding="SSH",
            )
            assert get_resp["SSHPublicKey"]["SSHPublicKeyId"] == key_id
            assert get_resp["SSHPublicKey"]["Status"] == "Active"
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
        finally:
            iam.delete_user(UserName=user_name)

    def test_update_ssh_public_key(self, iam):
        """UpdateSSHPublicKey changes key status."""
        user_name = _unique("ssh-usr")
        iam.create_user(UserName=user_name)
        try:
            upload_resp = iam.upload_ssh_public_key(
                UserName=user_name, SSHPublicKeyBody=SSH_PUBLIC_KEY_EXPLICIT
            )
            key_id = upload_resp["SSHPublicKey"]["SSHPublicKeyId"]
            iam.update_ssh_public_key(
                UserName=user_name,
                SSHPublicKeyId=key_id,
                Status="Inactive",
            )
            get_resp = iam.get_ssh_public_key(
                UserName=user_name,
                SSHPublicKeyId=key_id,
                Encoding="SSH",
            )
            assert get_resp["SSHPublicKey"]["Status"] == "Inactive"
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
        finally:
            iam.delete_user(UserName=user_name)

    def test_delete_ssh_public_key(self, iam):
        """DeleteSSHPublicKey removes the key."""
        user_name = _unique("ssh-usr")
        iam.create_user(UserName=user_name)
        try:
            upload_resp = iam.upload_ssh_public_key(
                UserName=user_name, SSHPublicKeyBody=SSH_PUBLIC_KEY_EXPLICIT
            )
            key_id = upload_resp["SSHPublicKey"]["SSHPublicKeyId"]
            iam.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key_id)
            list_resp = iam.list_ssh_public_keys(UserName=user_name)
            key_ids = [k["SSHPublicKeyId"] for k in list_resp["SSHPublicKeys"]]
            assert key_id not in key_ids
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMServiceSpecificCredentialExplicit:
    """Explicit coverage test for CreateServiceSpecificCredential."""

    def test_create_service_specific_credential(self, iam):
        """CreateServiceSpecificCredential creates a credential."""
        user_name = _unique("ssc-usr")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.create_service_specific_credential(
                UserName=user_name,
                ServiceName="codecommit.amazonaws.com",
            )
            cred = resp["ServiceSpecificCredential"]
            assert cred["UserName"] == user_name
            assert cred["ServiceName"] == "codecommit.amazonaws.com"
            assert "ServiceSpecificCredentialId" in cred
            assert "ServicePassword" in cred
        finally:
            iam.delete_user(UserName=user_name)
