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
                pass
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
                pass
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
                pass
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
                pass
            try:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=arn2)
            except Exception:
                pass
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
                pass
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
                pass
            iam.delete_role(RoleName=role_name)

    def test_list_role_policies(self, iam):
        role_name = _unique("lrp-role")
        p1 = _unique("lrp-pol1")
        p2 = _unique("lrp-pol2")
        try:
            iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=TRUST_POLICY)
            iam.put_role_policy(
                RoleName=role_name, PolicyName=p1, PolicyDocument=SIMPLE_POLICY_DOC
            )
            iam.put_role_policy(
                RoleName=role_name, PolicyName=p2, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.list_role_policies(RoleName=role_name)
            assert p1 in resp["PolicyNames"]
            assert p2 in resp["PolicyNames"]
        finally:
            for p in [p1, p2]:
                try:
                    iam.delete_role_policy(RoleName=role_name, PolicyName=p)
                except Exception:
                    pass
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
            iam.put_user_policy(
                UserName=user_name, PolicyName=p1, PolicyDocument=SIMPLE_POLICY_DOC
            )
            iam.put_user_policy(
                UserName=user_name, PolicyName=p2, PolicyDocument=SIMPLE_POLICY_DOC
            )
            resp = iam.list_user_policies(UserName=user_name)
            assert p1 in resp["PolicyNames"]
            assert p2 in resp["PolicyNames"]
        finally:
            for p in [p1, p2]:
                try:
                    iam.delete_user_policy(UserName=user_name, PolicyName=p)
                except Exception:
                    pass
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
                pass
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
                iam.delete_access_key(
                    UserName=user_name, AccessKeyId=ak["AccessKeyId"]
                )
            except Exception:
                pass
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
                iam.delete_access_key(
                    UserName=user_name, AccessKeyId=ak["AccessKeyId"]
                )
            except Exception:
                pass
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
                iam.delete_access_key(
                    UserName=user_name, AccessKeyId=ak["AccessKeyId"]
                )
            except Exception:
                pass
            iam.delete_user(UserName=user_name)

    def test_delete_access_key(self, iam):
        user_name = _unique("dak-usr")
        try:
            iam.create_user(UserName=user_name)
            ak = iam.create_access_key(UserName=user_name)["AccessKey"]
            iam.delete_access_key(
                UserName=user_name, AccessKeyId=ak["AccessKeyId"]
            )
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
                        pass
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
                pass
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
                pass
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
            iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
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
                pass
            iam.delete_instance_profile(InstanceProfileName=profile_name)
            iam.delete_role(RoleName=role_name)


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
                pass
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
                pass
        finally:
            try:
                iam.delete_login_profile(UserName=user_name)
            except Exception:
                pass
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
        """CreateServiceLinkedRole / DeleteServiceLinkedRole / GetServiceLinkedRoleDeletionStatus."""
        try:
            resp = iam.create_service_linked_role(AWSServiceName="elasticbeanstalk.amazonaws.com")
            role = resp["Role"]
            assert "elasticbeanstalk" in role["RoleName"].lower() or "AWSServiceRole" in role["Path"]
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
                pass


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
    @pytest.mark.xfail(reason="SAML provider may not be fully supported")
    def test_create_get_delete_saml_provider(self, iam):
        """CreateSAMLProvider / GetSAMLProvider / DeleteSAMLProvider."""
        saml_metadata = """<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
            entityID="https://idp.example.com">
            <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
                <SingleSignOnService
                    Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                    Location="https://idp.example.com/sso"/>
            </IDPSSODescriptor>
        </EntityDescriptor>"""
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
    @pytest.mark.xfail(reason="SimulatePrincipalPolicy may not be supported")
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

    @pytest.mark.xfail(reason="SimulateCustomPolicy may not be supported")
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


class TestIAMPolicyVersionsExtended:
    def test_create_list_get_delete_policy_version(self, iam):
        """CreatePolicyVersion / ListPolicyVersions / GetPolicyVersion / DeletePolicyVersion."""
        policy_name = _unique("pv-pol")
        pol = iam.create_policy(PolicyName=policy_name, PolicyDocument=SIMPLE_POLICY_DOC)
        arn = pol["Policy"]["Arn"]
        try:
            new_doc = json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            })
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
                pass


class TestIAMChangePassword:
    @pytest.mark.xfail(reason="ChangePassword requires actual user session")
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
    @pytest.mark.xfail(reason="ListSSHPublicKeys may not be supported")
    def test_list_ssh_public_keys(self, iam):
        """ListSSHPublicKeys."""
        user_name = _unique("ssh-user")
        iam.create_user(UserName=user_name)
        try:
            resp = iam.list_ssh_public_keys(UserName=user_name)
            assert "SSHPublicKeys" in resp
        finally:
            iam.delete_user(UserName=user_name)


class TestIAMUserTags:
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


class TestIAMRoleTags:
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
    @pytest.mark.xfail(reason="PutRolePermissionsBoundary may not be supported")
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
