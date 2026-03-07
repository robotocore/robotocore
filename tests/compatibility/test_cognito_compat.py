"""Cognito Identity Provider compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def cognito():
    return make_client("cognito-idp")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestCognitoUserPoolOperations:
    def test_create_user_pool(self, cognito):
        pool_name = _unique("test-pool")
        response = cognito.create_user_pool(PoolName=pool_name)
        pool = response["UserPool"]
        assert pool["Name"] == pool_name
        assert "Id" in pool
        cognito.delete_user_pool(UserPoolId=pool["Id"])

    def test_create_user_pool_client(self, cognito):
        pool_name = _unique("client-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            client_name = _unique("test-client")
            response = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName=client_name,
            )
            client = response["UserPoolClient"]
            assert client["ClientName"] == client_name
            assert "ClientId" in client
            assert client["UserPoolId"] == pool_id
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_user_pools(self, cognito):
        pool_name = _unique("list-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            response = cognito.list_user_pools(MaxResults=60)
            names = [p["Name"] for p in response["UserPools"]]
            assert pool_name in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_user_pool(self, cognito):
        pool_name = _unique("describe-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            response = cognito.describe_user_pool(UserPoolId=pool_id)
            assert response["UserPool"]["Name"] == pool_name
            assert response["UserPool"]["Id"] == pool_id
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


    def test_update_user_pool_client(self, cognito):
        pool_name = _unique("update-client-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            client_name = _unique("test-client")
            client = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName=client_name,
            )["UserPoolClient"]
            client_id = client["ClientId"]

            updated = cognito.update_user_pool_client(
                UserPoolId=pool_id,
                ClientId=client_id,
                ClientName="updated-client",
                ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
            )["UserPoolClient"]
            assert updated["ClientName"] == "updated-client"
            assert "ALLOW_USER_PASSWORD_AUTH" in updated["ExplicitAuthFlows"]

            # Verify via describe
            described = cognito.describe_user_pool_client(
                UserPoolId=pool_id,
                ClientId=client_id,
            )["UserPoolClient"]
            assert described["ClientName"] == "updated-client"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_create_and_get_group(self, cognito):
        pool_name = _unique("group-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            group_name = _unique("test-group")
            cognito.create_group(
                GroupName=group_name,
                UserPoolId=pool_id,
                Description="A test group",
            )
            group = cognito.get_group(
                GroupName=group_name,
                UserPoolId=pool_id,
            )["Group"]
            assert group["GroupName"] == group_name
            assert group["Description"] == "A test group"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_disable_enable_user(self, cognito):
        pool_name = _unique("disable-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("disableuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )

            # Disable the user
            cognito.admin_disable_user(UserPoolId=pool_id, Username=username)
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["Enabled"] is False

            # Re-enable the user
            cognito.admin_enable_user(UserPoolId=pool_id, Username=username)
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["Enabled"] is True
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoUserOperations:
    def test_admin_create_user(self, cognito):
        pool_name = _unique("admin-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("testuser")
            response = cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            user = response["User"]
            assert user["Username"] == username
            assert user["Enabled"] is True
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_get_user(self, cognito):
        pool_name = _unique("getuser-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("getuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            response = cognito.admin_get_user(
                UserPoolId=pool_id,
                Username=username,
            )
            assert response["Username"] == username
            assert response["Enabled"] is True
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_users(self, cognito):
        pool_name = _unique("listusers-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("listuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            response = cognito.list_users(UserPoolId=pool_id)
            usernames = [u["Username"] for u in response["Users"]]
            assert username in usernames
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_user_pool(self, cognito):
        pool_name = _unique("delete-pool")
        pool = cognito.create_user_pool(PoolName=pool_name)["UserPool"]
        pool_id = pool["Id"]
        cognito.delete_user_pool(UserPoolId=pool_id)
        # Verify it's gone
        response = cognito.list_user_pools(MaxResults=60)
        pool_ids = [p["Id"] for p in response["UserPools"]]
        assert pool_id not in pool_ids

    def test_admin_delete_user(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("del-user-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("deluser")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="TempPass1!"
            )
            cognito.admin_delete_user(UserPoolId=pool_id, Username=username)
            users = cognito.list_users(UserPoolId=pool_id)["Users"]
            assert username not in [u["Username"] for u in users]
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_create_user_with_attributes(self, cognito):
        pool = cognito.create_user_pool(
            PoolName=_unique("attr-pool"),
            Schema=[{"Name": "email", "AttributeDataType": "String", "Mutable": True}],
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("attruser")
            response = cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                UserAttributes=[{"Name": "email", "Value": "test@example.com"}],
            )
            attrs = {a["Name"]: a["Value"] for a in response["User"]["Attributes"]}
            assert attrs.get("email") == "test@example.com"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_groups(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("listgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_group(GroupName="group-a", UserPoolId=pool_id)
            cognito.create_group(GroupName="group-b", UserPoolId=pool_id)
            response = cognito.list_groups(UserPoolId=pool_id)
            names = [g["GroupName"] for g in response["Groups"]]
            assert "group-a" in names
            assert "group-b" in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_add_user_to_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("addgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("grpuser")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="TempPass1!"
            )
            cognito.create_group(GroupName="my-group", UserPoolId=pool_id)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=username, GroupName="my-group"
            )
            response = cognito.admin_list_groups_for_user(
                UserPoolId=pool_id, Username=username
            )
            groups = [g["GroupName"] for g in response["Groups"]]
            assert "my-group" in groups
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_user_pool_clients(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("listcli-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_user_pool_client(UserPoolId=pool_id, ClientName="client-a")
            cognito.create_user_pool_client(UserPoolId=pool_id, ClientName="client-b")
            response = cognito.list_user_pool_clients(UserPoolId=pool_id, MaxResults=10)
            names = [c["ClientName"] for c in response["UserPoolClients"]]
            assert "client-a" in names
            assert "client-b" in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_user_pool_client(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("desccli-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client = cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName="desc-client"
            )["UserPoolClient"]
            response = cognito.describe_user_pool_client(
                UserPoolId=pool_id, ClientId=client["ClientId"]
            )
            assert response["UserPoolClient"]["ClientName"] == "desc-client"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)
