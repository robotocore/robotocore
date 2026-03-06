"""IAM compatibility tests."""

import json

import pytest
from tests.compatibility.conftest import make_client


@pytest.fixture
def iam():
    return make_client("iam")


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
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        })
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
        policy_doc = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
        })
        response = iam.create_policy(PolicyName="test-policy", PolicyDocument=policy_doc)
        assert response["Policy"]["PolicyName"] == "test-policy"
        iam.delete_policy(PolicyArn=response["Policy"]["Arn"])

    def test_attach_role_policy(self, iam):
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        })
        iam.create_role(RoleName="attach-role", AssumeRolePolicyDocument=trust)
        policy_doc = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
        })
        pol = iam.create_policy(PolicyName="attach-policy", PolicyDocument=policy_doc)
        arn = pol["Policy"]["Arn"]
        iam.attach_role_policy(RoleName="attach-role", PolicyArn=arn)
        response = iam.list_attached_role_policies(RoleName="attach-role")
        assert any(p["PolicyArn"] == arn for p in response["AttachedPolicies"])
        iam.detach_role_policy(RoleName="attach-role", PolicyArn=arn)
        iam.delete_policy(PolicyArn=arn)
        iam.delete_role(RoleName="attach-role")
