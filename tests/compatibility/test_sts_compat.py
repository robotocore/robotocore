"""STS compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sts():
    return make_client("sts")


class TestSTSOperations:
    def test_get_caller_identity(self, sts):
        response = sts.get_caller_identity()
        assert "Account" in response
        assert "Arn" in response
        assert "UserId" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_session_token(self, sts):
        response = sts.get_session_token()
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds

    def test_assume_role(self, sts):
        import uuid

        role_name = f"test-sts-role-{uuid.uuid4().hex[:8]}"
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="test-session",
        )
        assert "Credentials" in response
        assert "AssumedRoleUser" in response
        iam.delete_role(RoleName=role_name)

    def test_assume_role_session_credentials(self, sts):
        """Verify assumed-role credentials contain all required fields."""
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName="test-creds-role",
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="creds-session",
        )
        creds = response["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
        assert "Expiration" in creds
        iam.delete_role(RoleName="test-creds-role")

    def test_get_access_key_info(self, sts):
        """Get account info for an access key."""
        response = sts.get_access_key_info(AccessKeyId="AKIAIOSFODNN7EXAMPLE")
        assert "Account" in response

    def test_assume_role_with_tags(self, sts):
        """Assume role with session tags."""
        import uuid

        role_name = f"test-tags-role-{uuid.uuid4().hex[:8]}"
        iam = make_client("iam")
        trust_policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"*"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="tag-session",
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        assert "Credentials" in response
        assert "AssumedRoleUser" in response
        iam.delete_role(RoleName=role_name)
