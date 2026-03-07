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


class TestCognitoUserPoolClients:
    def test_describe_user_pool_client(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("desc-client-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client = cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName=_unique("desc-client")
            )["UserPoolClient"]
            response = cognito.describe_user_pool_client(
                UserPoolId=pool_id, ClientId=client["ClientId"]
            )
            assert response["UserPoolClient"]["ClientName"] == client["ClientName"]
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_user_pool_clients(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("list-clients-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName=_unique("client-a")
            )
            cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName=_unique("client-b")
            )
            response = cognito.list_user_pool_clients(
                UserPoolId=pool_id, MaxResults=10
            )
            assert len(response["UserPoolClients"]) >= 2
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_pool_client(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("update-client-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client = cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName=_unique("upd-client")
            )["UserPoolClient"]
            updated = cognito.update_user_pool_client(
                UserPoolId=pool_id,
                ClientId=client["ClientId"],
                ClientName="updated-name",
            )
            assert updated["UserPoolClient"]["ClientName"] == "updated-name"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_user_pool_client(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("del-client-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client = cognito.create_user_pool_client(
                UserPoolId=pool_id, ClientName=_unique("del-client")
            )["UserPoolClient"]
            cognito.delete_user_pool_client(
                UserPoolId=pool_id, ClientId=client["ClientId"]
            )
            response = cognito.list_user_pool_clients(
                UserPoolId=pool_id, MaxResults=10
            )
            client_ids = [c["ClientId"] for c in response["UserPoolClients"]]
            assert client["ClientId"] not in client_ids
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoGroups:
    def test_create_and_get_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("group-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            group_name = _unique("test-group")
            cognito.create_group(
                UserPoolId=pool_id,
                GroupName=group_name,
                Description="Test group",
            )
            response = cognito.get_group(
                UserPoolId=pool_id, GroupName=group_name
            )
            assert response["Group"]["GroupName"] == group_name
            assert response["Group"]["Description"] == "Test group"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_groups(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("listgroup-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_group(UserPoolId=pool_id, GroupName=_unique("grp-a"))
            cognito.create_group(UserPoolId=pool_id, GroupName=_unique("grp-b"))
            response = cognito.list_groups(UserPoolId=pool_id)
            assert len(response["Groups"]) >= 2
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_add_user_to_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("usergrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("grpuser")
            group_name = _unique("grp")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            cognito.create_group(UserPoolId=pool_id, GroupName=group_name)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id,
                Username=username,
                GroupName=group_name,
            )
            response = cognito.admin_list_groups_for_user(
                UserPoolId=pool_id, Username=username
            )
            group_names = [g["GroupName"] for g in response["Groups"]]
            assert group_name in group_names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoAdminUserOps:
    def test_admin_disable_enable_user(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("disable-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("disableuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            cognito.admin_disable_user(UserPoolId=pool_id, Username=username)
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["Enabled"] is False

            cognito.admin_enable_user(UserPoolId=pool_id, Username=username)
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["Enabled"] is True
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_delete_user(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("deluser-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("deluser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            cognito.admin_delete_user(UserPoolId=pool_id, Username=username)
            response = cognito.list_users(UserPoolId=pool_id)
            usernames = [u["Username"] for u in response["Users"]]
            assert username not in usernames
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_set_user_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("setpw-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("pwuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
            )
            cognito.admin_set_user_password(
                UserPoolId=pool_id,
                Username=username,
                Password="NewPermanent1!",
                Permanent=True,
            )
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["UserStatus"] == "CONFIRMED"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)
