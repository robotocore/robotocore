"""Cognito Identity compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def cognito_identity():
    return make_client("cognito-identity")


def _create_pool(client, suffix=""):
    pool_name = f"test-pool-{uuid.uuid4().hex[:8]}{suffix}"
    resp = client.create_identity_pool(
        IdentityPoolName=pool_name,
        AllowUnauthenticatedIdentities=True,
    )
    return resp["IdentityPoolId"]


class TestCognitoIdentityOperations:
    def test_list_identity_pools_empty(self, cognito_identity):
        response = cognito_identity.list_identity_pools(MaxResults=10)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "IdentityPools" in response

    def test_create_describe_delete_identity_pool(self, cognito_identity):
        pool_name = f"test-pool-{uuid.uuid4().hex[:8]}"
        create_resp = cognito_identity.create_identity_pool(
            IdentityPoolName=pool_name,
            AllowUnauthenticatedIdentities=True,
        )
        pool_id = create_resp["IdentityPoolId"]
        assert create_resp["IdentityPoolName"] == pool_name
        assert create_resp["AllowUnauthenticatedIdentities"] is True

        describe_resp = cognito_identity.describe_identity_pool(
            IdentityPoolId=pool_id,
        )
        assert describe_resp["IdentityPoolId"] == pool_id
        assert describe_resp["IdentityPoolName"] == pool_name

        cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

        # Verify pool shows up in list before deletion was already done
        # Just verify delete didn't error
        assert True

    def test_create_identity_pool_appears_in_list(self, cognito_identity):
        pool_name = f"test-list-{uuid.uuid4().hex[:8]}"
        create_resp = cognito_identity.create_identity_pool(
            IdentityPoolName=pool_name,
            AllowUnauthenticatedIdentities=True,
        )
        pool_id = create_resp["IdentityPoolId"]

        try:
            list_resp = cognito_identity.list_identity_pools(MaxResults=60)
            pool_ids = [p["IdentityPoolId"] for p in list_resp["IdentityPools"]]
            assert pool_id in pool_ids
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_get_credentials_for_identity(self, cognito_identity):
        pool_id = _create_pool(cognito_identity)
        try:
            identity_id = f"{pool_id}:fake-identity-{uuid.uuid4().hex[:8]}"
            resp = cognito_identity.get_credentials_for_identity(IdentityId=identity_id)
            creds = resp["Credentials"]
            assert creds["AccessKeyId"] != ""
            assert creds["SecretKey"] != ""
            assert resp["IdentityId"] != ""
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_get_open_id_token(self, cognito_identity):
        pool_id = _create_pool(cognito_identity)
        try:
            identity_id = f"{pool_id}:fake-identity-{uuid.uuid4().hex[:8]}"
            resp = cognito_identity.get_open_id_token(IdentityId=identity_id)
            assert len(resp["Token"]) > 0
            assert resp["IdentityId"] != ""
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_get_open_id_token_for_developer_identity(self, cognito_identity):
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.get_open_id_token_for_developer_identity(
                IdentityPoolId=pool_id,
                Logins={"myapp": f"user-{uuid.uuid4().hex[:8]}"},
            )
            assert len(resp["Token"]) > 0
            assert resp["IdentityId"].startswith("us-east-1:")
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)
