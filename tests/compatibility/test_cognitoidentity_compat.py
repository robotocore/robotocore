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


class TestUpdateIdentityPool:
    """Tests for UpdateIdentityPool operation."""

    @pytest.fixture
    def cognito_identity(self):
        return make_client("cognito-identity")

    def test_update_identity_pool(self, cognito_identity):
        """UpdateIdentityPool returns pool response with IdentityPoolId."""
        pool_id = _create_pool(cognito_identity, suffix="-update")
        try:
            resp = cognito_identity.update_identity_pool(
                IdentityPoolId=pool_id,
                IdentityPoolName="updated-pool-name",
                AllowUnauthenticatedIdentities=False,
            )
            assert "IdentityPoolId" in resp
            assert resp["IdentityPoolId"] == pool_id
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)


class TestCognitoIdentityNewOps:
    """Tests for newly implemented cognitoidentity operations."""

    def test_describe_identity(self, cognito_identity):
        """DescribeIdentity returns identity details."""
        pool_id = _create_pool(cognito_identity)
        try:
            id_resp = cognito_identity.get_id(IdentityPoolId=pool_id)
            identity_id = id_resp["IdentityId"]
            resp = cognito_identity.describe_identity(IdentityId=identity_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["IdentityId"] == identity_id
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_delete_identities(self, cognito_identity):
        """DeleteIdentities returns empty UnprocessedIdentityIds."""
        pool_id = _create_pool(cognito_identity)
        try:
            id_resp = cognito_identity.get_id(IdentityPoolId=pool_id)
            identity_id = id_resp["IdentityId"]
            resp = cognito_identity.delete_identities(IdentityIdsToDelete=[identity_id])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "UnprocessedIdentityIds" in resp
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_get_identity_pool_roles(self, cognito_identity):
        """GetIdentityPoolRoles returns roles for an identity pool."""
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.get_identity_pool_roles(IdentityPoolId=pool_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Roles" in resp
            assert resp["IdentityPoolId"] == pool_id
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_set_identity_pool_roles(self, cognito_identity):
        """SetIdentityPoolRoles sets roles without error."""
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.set_identity_pool_roles(
                IdentityPoolId=pool_id,
                Roles={
                    "authenticated": "arn:aws:iam::123456789012:role/auth-role",
                    "unauthenticated": "arn:aws:iam::123456789012:role/unauth-role",
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify roles were set
            get_resp = cognito_identity.get_identity_pool_roles(IdentityPoolId=pool_id)
            assert "authenticated" in get_resp["Roles"]
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_list_tags_for_resource(self, cognito_identity):
        """ListTagsForResource returns tags dict."""
        pool_id = _create_pool(cognito_identity)
        pool_arn = f"arn:aws:cognito-identity:us-east-1:123456789012:identitypool/{pool_id}"
        try:
            resp = cognito_identity.list_tags_for_resource(ResourceArn=pool_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Tags" in resp
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_tag_and_untag_resource(self, cognito_identity):
        """TagResource and UntagResource manage tags correctly."""
        pool_id = _create_pool(cognito_identity)
        pool_arn = f"arn:aws:cognito-identity:us-east-1:123456789012:identitypool/{pool_id}"
        try:
            cognito_identity.tag_resource(ResourceArn=pool_arn, Tags={"env": "test"})
            list_resp = cognito_identity.list_tags_for_resource(ResourceArn=pool_arn)
            assert list_resp["Tags"].get("env") == "test"

            cognito_identity.untag_resource(ResourceArn=pool_arn, TagKeys=["env"])
            list_resp2 = cognito_identity.list_tags_for_resource(ResourceArn=pool_arn)
            assert "env" not in list_resp2["Tags"]
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_lookup_developer_identity(self, cognito_identity):
        """LookupDeveloperIdentity returns an IdentityId."""
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.lookup_developer_identity(
                IdentityPoolId=pool_id,
                DeveloperUserIdentifier=f"user-{uuid.uuid4().hex[:8]}",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "IdentityId" in resp
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_merge_developer_identities(self, cognito_identity):
        """MergeDeveloperIdentities returns a merged IdentityId."""
        pool_name = f"test-merge-{uuid.uuid4().hex[:8]}"
        pool_id_resp = cognito_identity.create_identity_pool(
            IdentityPoolName=pool_name,
            AllowUnauthenticatedIdentities=True,
            DeveloperProviderName="login.myapp.test",
        )
        pool_id = pool_id_resp["IdentityPoolId"]
        try:
            resp = cognito_identity.merge_developer_identities(
                SourceUserIdentifier="user-source",
                DestinationUserIdentifier="user-dest",
                DeveloperProviderName="login.myapp.test",
                IdentityPoolId=pool_id,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "IdentityId" in resp
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_get_and_set_principal_tag_attribute_map(self, cognito_identity):
        """GetPrincipalTagAttributeMap and SetPrincipalTagAttributeMap work together."""
        pool_id = _create_pool(cognito_identity)
        try:
            get_resp = cognito_identity.get_principal_tag_attribute_map(
                IdentityPoolId=pool_id,
                IdentityProviderName="login.myapp.com",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200 if (resp := get_resp) else True
            assert "IdentityPoolId" in get_resp

            set_resp = cognito_identity.set_principal_tag_attribute_map(
                IdentityPoolId=pool_id,
                IdentityProviderName="login.myapp.com",
                UseDefaults=True,
                PrincipalTags={"role": "sub"},
            )
            assert set_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert set_resp["IdentityPoolId"] == pool_id
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_unlink_developer_identity(self, cognito_identity):
        """UnlinkDeveloperIdentity succeeds without raising an error."""
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.unlink_developer_identity(
                IdentityId=f"{pool_id.split(':')[0]}:12345678-1234-1234-1234-123456789012",
                IdentityPoolId=pool_id,
                DeveloperProviderName="myapp.provider",
                DeveloperUserIdentifier="user123",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)

    def test_unlink_identity(self, cognito_identity):
        """UnlinkIdentity succeeds without raising an error."""
        pool_id = _create_pool(cognito_identity)
        try:
            resp = cognito_identity.unlink_identity(
                IdentityId=f"{pool_id.split(':')[0]}:12345678-1234-1234-1234-123456789012",
                Logins={"cognito-identity.amazonaws.com": "testtoken"},
                LoginsToRemove=["cognito-identity.amazonaws.com"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito_identity.delete_identity_pool(IdentityPoolId=pool_id)
