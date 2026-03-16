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
            response = cognito.admin_list_groups_for_user(UserPoolId=pool_id, Username=username)
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
            client = cognito.create_user_pool_client(UserPoolId=pool_id, ClientName="desc-client")[
                "UserPoolClient"
            ]
            response = cognito.describe_user_pool_client(
                UserPoolId=pool_id, ClientId=client["ClientId"]
            )
            assert response["UserPoolClient"]["ClientName"] == "desc-client"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoExtended:
    """Extended Cognito operations for higher coverage."""

    @pytest.fixture
    def cognito(self):
        from tests.compatibility.conftest import make_client

        return make_client("cognito-idp")

    def test_admin_set_user_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("setpw-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("pw-user")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="Temp1234!"
            )
            cognito.admin_set_user_password(
                UserPoolId=pool_id,
                Username=username,
                Password="Permanent1!",
                Permanent=True,
            )
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["UserStatus"] == "CONFIRMED"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_disable_enable_user(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("toggle-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("toggle-user")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="Temp1234!"
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
        pool = cognito.create_user_pool(PoolName=_unique("del-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("del-user")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="Temp1234!"
            )
            cognito.admin_delete_user(UserPoolId=pool_id, Username=username)
            resp = cognito.list_users(UserPoolId=pool_id)
            usernames = [u["Username"] for u in resp["Users"]]
            assert username not in usernames
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_pool(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("upd-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.update_user_pool(
                UserPoolId=pool_id,
                AutoVerifiedAttributes=["email"],
            )
            desc = cognito.describe_user_pool(UserPoolId=pool_id)
            assert "email" in desc["UserPool"].get("AutoVerifiedAttributes", [])
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_pool_client(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("updcli-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client = cognito.create_user_pool_client(UserPoolId=pool_id, ClientName="upd-client")[
                "UserPoolClient"
            ]
            cognito.update_user_pool_client(
                UserPoolId=pool_id,
                ClientId=client["ClientId"],
                ClientName="renamed-client",
            )
            desc = cognito.describe_user_pool_client(
                UserPoolId=pool_id, ClientId=client["ClientId"]
            )
            assert desc["UserPoolClient"]["ClientName"] == "renamed-client"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_update_user_attributes(self, cognito):
        pool = cognito.create_user_pool(
            PoolName=_unique("attr-pool"),
            Schema=[{"Name": "email", "AttributeDataType": "String", "Mutable": True}],
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("attr-user")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="Temp1234!"
            )
            cognito.admin_update_user_attributes(
                UserPoolId=pool_id,
                Username=username,
                UserAttributes=[{"Name": "email", "Value": "test@example.com"}],
            )
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            attrs = {a["Name"]: a["Value"] for a in user.get("UserAttributes", [])}
            assert attrs.get("email") == "test@example.com"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_remove_user_from_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("rmgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("rmgrp-user")
            cognito.admin_create_user(
                UserPoolId=pool_id, Username=username, TemporaryPassword="Temp1234!"
            )
            cognito.create_group(GroupName="rm-group", UserPoolId=pool_id)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=username, GroupName="rm-group"
            )
            cognito.admin_remove_user_from_group(
                UserPoolId=pool_id, Username=username, GroupName="rm-group"
            )
            resp = cognito.admin_list_groups_for_user(UserPoolId=pool_id, Username=username)
            groups = [g["GroupName"] for g in resp["Groups"]]
            assert "rm-group" not in groups
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("delgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_group(GroupName="to-delete", UserPoolId=pool_id)
            cognito.delete_group(GroupName="to-delete", UserPoolId=pool_id)
            resp = cognito.list_groups(UserPoolId=pool_id)
            names = [g["GroupName"] for g in resp["Groups"]]
            assert "to-delete" not in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_get_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("getgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_group(
                GroupName="info-group", UserPoolId=pool_id, Description="Test group"
            )
            resp = cognito.get_group(GroupName="info-group", UserPoolId=pool_id)
            assert resp["Group"]["GroupName"] == "info-group"
            assert resp["Group"].get("Description") == "Test group"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoExtendedV2:
    """Extended Cognito compatibility tests covering additional operations."""

    def test_create_user_pool_with_password_policy(self, cognito):
        pool_name = _unique("pwpol-pool")
        response = cognito.create_user_pool(
            PoolName=pool_name,
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 12,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": False,
                    "TemporaryPasswordValidityDays": 3,
                }
            },
        )
        pool = response["UserPool"]
        pool_id = pool["Id"]
        try:
            policy = pool["Policies"]["PasswordPolicy"]
            assert policy["MinimumLength"] == 12
            assert policy["RequireUppercase"] is True
            assert policy["RequireLowercase"] is True
            assert policy["RequireNumbers"] is True
            assert policy["RequireSymbols"] is False
            assert policy["TemporaryPasswordValidityDays"] == 3
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_create_user_pool_with_mfa_config(self, cognito):
        pool_name = _unique("mfa-pool")
        response = cognito.create_user_pool(
            PoolName=pool_name,
            MfaConfiguration="OPTIONAL",
        )
        pool = response["UserPool"]
        pool_id = pool["Id"]
        try:
            assert pool["MfaConfiguration"] == "OPTIONAL"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_create_user_pool_with_email_config(self, cognito):
        pool_name = _unique("email-pool")
        response = cognito.create_user_pool(
            PoolName=pool_name,
            EmailConfiguration={
                "EmailSendingAccount": "COGNITO_DEFAULT",
            },
        )
        pool = response["UserPool"]
        pool_id = pool["Id"]
        try:
            assert pool["EmailConfiguration"]["EmailSendingAccount"] == "COGNITO_DEFAULT"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_create_user_pool_with_lambda_triggers(self, cognito):
        pool_name = _unique("trigger-pool")
        # Use a fake ARN - we just need to verify it round-trips
        fake_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-trigger"
        response = cognito.create_user_pool(
            PoolName=pool_name,
            LambdaConfig={
                "PreSignUp": fake_arn,
            },
        )
        pool = response["UserPool"]
        pool_id = pool["Id"]
        try:
            assert pool["LambdaConfig"]["PreSignUp"] == fake_arn
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_pool(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("upd-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.update_user_pool(
                UserPoolId=pool_id,
                Policies={
                    "PasswordPolicy": {
                        "MinimumLength": 16,
                        "RequireUppercase": True,
                        "RequireLowercase": True,
                        "RequireNumbers": True,
                        "RequireSymbols": True,
                    }
                },
                AutoVerifiedAttributes=["email"],
            )
            described = cognito.describe_user_pool(UserPoolId=pool_id)["UserPool"]
            policy = described["Policies"]["PasswordPolicy"]
            assert policy["MinimumLength"] == 16
            assert policy["RequireSymbols"] is True
            assert "email" in described.get("AutoVerifiedAttributes", [])
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_user_pool_fields(self, cognito):
        pool_name = _unique("fields-pool")
        pool = cognito.create_user_pool(
            PoolName=pool_name,
            MfaConfiguration="OFF",
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            described = cognito.describe_user_pool(UserPoolId=pool_id)["UserPool"]
            assert described["Name"] == pool_name
            assert described["Id"] == pool_id
            assert "CreationDate" in described
            assert "LastModifiedDate" in described
            assert "Arn" in described
            assert described["MfaConfiguration"] == "OFF"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_create_user_with_temporary_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("tmppass-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("tmpuser")
            response = cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="MyTemp!Pass123",
                MessageAction="SUPPRESS",
            )
            user = response["User"]
            assert user["Username"] == username
            assert user["UserStatus"] == "FORCE_CHANGE_PASSWORD"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_set_user_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("setpw-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("setpwuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            cognito.admin_set_user_password(
                UserPoolId=pool_id,
                Username=username,
                Password="NewPermanent!Pass1",
                Permanent=True,
            )
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert user["UserStatus"] == "CONFIRMED"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_get_user_verify_attributes(self, cognito):
        pool = cognito.create_user_pool(
            PoolName=_unique("getattr-pool"),
            Schema=[
                {"Name": "email", "AttributeDataType": "String", "Mutable": True},
                {"Name": "phone_number", "AttributeDataType": "String", "Mutable": True},
            ],
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("attrchk")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                UserAttributes=[
                    {"Name": "email", "Value": "verify@example.com"},
                    {"Name": "phone_number", "Value": "+15551234567"},
                ],
                MessageAction="SUPPRESS",
            )
            user = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
            assert attrs["email"] == "verify@example.com"
            assert attrs["phone_number"] == "+15551234567"
            assert "UserCreateDate" in user
            assert "UserLastModifiedDate" in user
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_delete_user_then_not_found(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("delnf-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("delnfuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            cognito.admin_delete_user(UserPoolId=pool_id, Username=username)
            with pytest.raises(Exception) as exc_info:
                cognito.admin_get_user(UserPoolId=pool_id, Username=username)
            assert "UserNotFoundException" in str(
                type(exc_info.value).__name__
            ) or "UserNotFoundException" in str(exc_info.value)
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_remove_user_from_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("rmgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("rmgrpuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            group_name = _unique("rmgroup")
            cognito.create_group(GroupName=group_name, UserPoolId=pool_id)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=username, GroupName=group_name
            )
            # Verify user is in group
            groups = cognito.admin_list_groups_for_user(UserPoolId=pool_id, Username=username)[
                "Groups"
            ]
            assert group_name in [g["GroupName"] for g in groups]

            # Remove user from group
            cognito.admin_remove_user_from_group(
                UserPoolId=pool_id, Username=username, GroupName=group_name
            )
            groups = cognito.admin_list_groups_for_user(UserPoolId=pool_id, Username=username)[
                "Groups"
            ]
            assert group_name not in [g["GroupName"] for g in groups]
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_list_groups_for_user_multiple_groups(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("mulgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("mulgrpuser")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            group_a = _unique("grpA")
            group_b = _unique("grpB")
            cognito.create_group(GroupName=group_a, UserPoolId=pool_id)
            cognito.create_group(GroupName=group_b, UserPoolId=pool_id)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=username, GroupName=group_a
            )
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=username, GroupName=group_b
            )
            groups = cognito.admin_list_groups_for_user(UserPoolId=pool_id, Username=username)[
                "Groups"
            ]
            group_names = [g["GroupName"] for g in groups]
            assert group_a in group_names
            assert group_b in group_names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_create_group_with_description_and_precedence(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("grpprec-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            group_name = _unique("precgrp")
            response = cognito.create_group(
                GroupName=group_name,
                UserPoolId=pool_id,
                Description="Admin group",
                Precedence=1,
            )
            group = response["Group"]
            assert group["GroupName"] == group_name
            assert group["Description"] == "Admin group"
            assert group["Precedence"] == 1
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("updgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            group_name = _unique("updgrp")
            cognito.create_group(
                GroupName=group_name,
                UserPoolId=pool_id,
                Description="Original",
                Precedence=10,
            )
            response = cognito.update_group(
                GroupName=group_name,
                UserPoolId=pool_id,
                Description="Updated description",
                Precedence=5,
            )
            group = response["Group"]
            assert group["Description"] == "Updated description"
            assert group["Precedence"] == 5
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_groups_with_precedence_ordering(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("ordgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_group(GroupName="high-prec", UserPoolId=pool_id, Precedence=1)
            cognito.create_group(GroupName="low-prec", UserPoolId=pool_id, Precedence=100)
            cognito.create_group(GroupName="mid-prec", UserPoolId=pool_id, Precedence=50)
            groups = cognito.list_groups(UserPoolId=pool_id)["Groups"]
            group_names = [g["GroupName"] for g in groups]
            assert "high-prec" in group_names
            assert "mid-prec" in group_names
            assert "low-prec" in group_names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_add_custom_attributes(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("custattr-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.add_custom_attributes(
                UserPoolId=pool_id,
                CustomAttributes=[
                    {
                        "Name": "department",
                        "AttributeDataType": "String",
                        "Mutable": True,
                        "StringAttributeConstraints": {
                            "MinLength": "1",
                            "MaxLength": "100",
                        },
                    },
                    {
                        "Name": "employee_id",
                        "AttributeDataType": "Number",
                        "Mutable": False,
                        "NumberAttributeConstraints": {
                            "MinValue": "1",
                            "MaxValue": "999999",
                        },
                    },
                ],
            )
            # Verify by describing the pool and checking schema
            described = cognito.describe_user_pool(UserPoolId=pool_id)["UserPool"]
            schema_names = [attr["Name"] for attr in described.get("SchemaAttributes", [])]
            assert "custom:department" in schema_names
            assert "custom:employee_id" in schema_names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_set_user_pool_mfa_config(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("mfacfg-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            response = cognito.set_user_pool_mfa_config(
                UserPoolId=pool_id,
                SoftwareTokenMfaConfiguration={"Enabled": True},
                MfaConfiguration="OPTIONAL",
            )
            assert response["MfaConfiguration"] == "OPTIONAL"
            assert response["SoftwareTokenMfaConfiguration"]["Enabled"] is True
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("delgrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            group_name = _unique("delgrp")
            cognito.create_group(GroupName=group_name, UserPoolId=pool_id)
            cognito.delete_group(GroupName=group_name, UserPoolId=pool_id)
            groups = cognito.list_groups(UserPoolId=pool_id)["Groups"]
            assert group_name not in [g["GroupName"] for g in groups]
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_users_in_group(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("usringrp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            user1 = _unique("grpusr1")
            user2 = _unique("grpusr2")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=user1,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=user2,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            group_name = _unique("usrgrp")
            cognito.create_group(GroupName=group_name, UserPoolId=pool_id)
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=user1, GroupName=group_name
            )
            cognito.admin_add_user_to_group(
                UserPoolId=pool_id, Username=user2, GroupName=group_name
            )
            response = cognito.list_users_in_group(UserPoolId=pool_id, GroupName=group_name)
            usernames = [u["Username"] for u in response["Users"]]
            assert user1 in usernames
            assert user2 in usernames
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoAdditionalOps:
    """Tests for additional cognito-idp operations."""

    @pytest.fixture
    def cognito(self):
        from tests.compatibility.conftest import make_client

        return make_client("cognito-idp")

    @pytest.fixture
    def pool(self, cognito):
        """Create a user pool for tests and clean up after."""
        pool_name = _unique("addlops-pool")
        response = cognito.create_user_pool(PoolName=pool_name)
        pool_id = response["UserPool"]["Id"]
        yield {"id": pool_id, "name": pool_name, "arn": response["UserPool"]["Arn"]}
        cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_tags_for_resource(self, cognito, pool):
        response = cognito.list_tags_for_resource(ResourceArn=pool["arn"])
        assert "Tags" in response
        assert isinstance(response["Tags"], dict)

    def test_list_tags_for_resource_with_tags(self, cognito, pool):
        cognito.tag_resource(ResourceArn=pool["arn"], Tags={"env": "test", "team": "backend"})
        response = cognito.list_tags_for_resource(ResourceArn=pool["arn"])
        assert response["Tags"]["env"] == "test"
        assert response["Tags"]["team"] == "backend"

    def test_describe_user_pool_domain_not_found(self, cognito):
        response = cognito.describe_user_pool_domain(Domain="nonexistent-domain-xyz-12345")
        assert "DomainDescription" in response

    def test_get_user_requires_auth(self, cognito):
        """GetUser requires an access token; calling with a fake token should error."""
        with pytest.raises(Exception) as exc_info:
            cognito.get_user(AccessToken="fake-access-token-12345")
        err_str = str(type(exc_info.value).__name__) + str(exc_info.value)
        assert "NotAuthorizedException" in err_str

    def test_untag_resource(self, cognito, pool):
        cognito.tag_resource(ResourceArn=pool["arn"], Tags={"k1": "v1", "k2": "v2"})
        cognito.untag_resource(ResourceArn=pool["arn"], TagKeys=["k1"])
        response = cognito.list_tags_for_resource(ResourceArn=pool["arn"])
        assert "k1" not in response["Tags"]
        assert response["Tags"]["k2"] == "v2"

    def test_delete_user_pool_client(self, cognito, pool):
        client = cognito.create_user_pool_client(UserPoolId=pool["id"], ClientName="to-delete")[
            "UserPoolClient"
        ]
        cognito.delete_user_pool_client(UserPoolId=pool["id"], ClientId=client["ClientId"])
        clients = cognito.list_user_pool_clients(UserPoolId=pool["id"], MaxResults=60)[
            "UserPoolClients"
        ]
        client_ids = [c["ClientId"] for c in clients]
        assert client["ClientId"] not in client_ids


class TestCognitoAuthFlows:
    """Tests for authentication flows: SignUp, InitiateAuth, etc."""

    @pytest.fixture
    def cognito(self):
        from tests.compatibility.conftest import make_client

        return make_client("cognito-idp")

    @pytest.fixture
    def auth_pool(self, cognito):
        """Pool configured for USER_PASSWORD_AUTH with a client."""
        pool_name = _unique("auth-pool")
        pool = cognito.create_user_pool(
            PoolName=pool_name,
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                }
            },
            AutoVerifiedAttributes=["email"],
            Schema=[
                {"Name": "email", "AttributeDataType": "String", "Mutable": True},
            ],
        )["UserPool"]
        pool_id = pool["Id"]
        client = cognito.create_user_pool_client(
            UserPoolId=pool_id,
            ClientName="auth-client",
            ExplicitAuthFlows=[
                "ALLOW_USER_PASSWORD_AUTH",
                "ALLOW_REFRESH_TOKEN_AUTH",
                "ALLOW_ADMIN_USER_PASSWORD_AUTH",
            ],
        )["UserPoolClient"]
        yield {
            "pool_id": pool_id,
            "client_id": client["ClientId"],
        }
        cognito.delete_user_pool(UserPoolId=pool_id)

    def test_sign_up(self, cognito, auth_pool):
        username = _unique("signup-user")
        response = cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password="Test@12345678",
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        assert response["UserConfirmed"] is False
        assert "UserSub" in response

    def test_admin_confirm_sign_up(self, cognito, auth_pool):
        username = _unique("confirm-user")
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password="Test@12345678",
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        user = cognito.admin_get_user(UserPoolId=auth_pool["pool_id"], Username=username)
        assert user["UserStatus"] == "CONFIRMED"

    def test_initiate_auth_user_password(self, cognito, auth_pool):
        username = _unique("initauth-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        response = cognito.initiate_auth(
            ClientId=auth_pool["client_id"],
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
        )
        assert "AuthenticationResult" in response
        assert "AccessToken" in response["AuthenticationResult"]
        assert "IdToken" in response["AuthenticationResult"]
        assert "RefreshToken" in response["AuthenticationResult"]

    def test_admin_initiate_auth(self, cognito, auth_pool):
        username = _unique("adminauth-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        response = cognito.admin_initiate_auth(
            UserPoolId=auth_pool["pool_id"],
            ClientId=auth_pool["client_id"],
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
        )
        assert "AuthenticationResult" in response
        assert "AccessToken" in response["AuthenticationResult"]

    def test_get_user_with_access_token(self, cognito, auth_pool):
        """GetUser with a valid access token from authentication."""
        username = _unique("getuser-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        auth = cognito.initiate_auth(
            ClientId=auth_pool["client_id"],
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
        )
        access_token = auth["AuthenticationResult"]["AccessToken"]
        response = cognito.get_user(AccessToken=access_token)
        assert response["Username"] == username

    def test_change_password(self, cognito, auth_pool):
        username = _unique("chgpw-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        auth = cognito.initiate_auth(
            ClientId=auth_pool["client_id"],
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
        )
        access_token = auth["AuthenticationResult"]["AccessToken"]
        response = cognito.change_password(
            PreviousPassword=password,
            ProposedPassword="NewTest@12345678",
            AccessToken=access_token,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_forgot_password(self, cognito, auth_pool):
        username = _unique("forgot-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        response = cognito.forgot_password(ClientId=auth_pool["client_id"], Username=username)
        assert "CodeDeliveryDetails" in response

    def test_confirm_forgot_password(self, cognito, auth_pool):
        """ConfirmForgotPassword with a confirmation code (Moto accepts any code)."""
        username = _unique("conforgot-user")
        password = "Test@12345678"
        cognito.sign_up(
            ClientId=auth_pool["client_id"],
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
        )
        cognito.admin_confirm_sign_up(UserPoolId=auth_pool["pool_id"], Username=username)
        cognito.forgot_password(ClientId=auth_pool["client_id"], Username=username)
        response = cognito.confirm_forgot_password(
            ClientId=auth_pool["client_id"],
            Username=username,
            ConfirmationCode="123456",
            Password="NewTest@12345678",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGetUserPoolMfaConfig:
    def test_get_user_pool_mfa_config_round_trip(self):
        client = make_client("cognito-idp")
        suffix = uuid.uuid4().hex[:8]
        pool_name = f"mfa-pool-{suffix}"

        pool_resp = client.create_user_pool(PoolName=pool_name)
        pool_id = pool_resp["UserPool"]["Id"]

        client.set_user_pool_mfa_config(
            UserPoolId=pool_id,
            MfaConfiguration="ON",
            SoftwareTokenMfaConfiguration={"Enabled": True},
        )

        get_resp = client.get_user_pool_mfa_config(UserPoolId=pool_id)
        assert get_resp["MfaConfiguration"] == "ON"
        assert get_resp["SoftwareTokenMfaConfiguration"]["Enabled"] is True

        client.delete_user_pool(UserPoolId=pool_id)

    def test_get_mfa_config_default_off(self):
        """New user pools have MFA OFF by default."""
        client = make_client("cognito-idp")
        suffix = uuid.uuid4().hex[:8]
        pool_resp = client.create_user_pool(PoolName=f"mfa-off-{suffix}")
        pool_id = pool_resp["UserPool"]["Id"]

        get_resp = client.get_user_pool_mfa_config(UserPoolId=pool_id)
        assert get_resp["MfaConfiguration"] == "OFF"

        client.delete_user_pool(UserPoolId=pool_id)


class TestCognitoIdentityProviders:
    """Tests for identity provider CRUD operations."""

    def test_create_identity_provider(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("idp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            resp = cognito.create_identity_provider(
                UserPoolId=pool_id,
                ProviderName="TestOIDC",
                ProviderType="OIDC",
                ProviderDetails={
                    "client_id": "test-client-id",
                    "authorize_scopes": "openid email",
                    "oidc_issuer": "https://example.com",
                },
            )
            assert resp["IdentityProvider"]["ProviderName"] == "TestOIDC"
            assert resp["IdentityProvider"]["ProviderType"] == "OIDC"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_identity_provider(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("didp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_identity_provider(
                UserPoolId=pool_id,
                ProviderName="DescOIDC",
                ProviderType="OIDC",
                ProviderDetails={
                    "client_id": "test-client",
                    "authorize_scopes": "openid",
                    "oidc_issuer": "https://example.com",
                },
            )
            resp = cognito.describe_identity_provider(UserPoolId=pool_id, ProviderName="DescOIDC")
            assert resp["IdentityProvider"]["ProviderName"] == "DescOIDC"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_list_identity_providers(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("lidp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_identity_provider(
                UserPoolId=pool_id,
                ProviderName="ListOIDC",
                ProviderType="OIDC",
                ProviderDetails={
                    "client_id": "test-client",
                    "authorize_scopes": "openid",
                    "oidc_issuer": "https://example.com",
                },
            )
            resp = cognito.list_identity_providers(UserPoolId=pool_id, MaxResults=10)
            names = [p["ProviderName"] for p in resp["Providers"]]
            assert "ListOIDC" in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_identity_provider(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("uidp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_identity_provider(
                UserPoolId=pool_id,
                ProviderName="UpdOIDC",
                ProviderType="OIDC",
                ProviderDetails={
                    "client_id": "old-client",
                    "authorize_scopes": "openid",
                    "oidc_issuer": "https://example.com",
                },
            )
            resp = cognito.update_identity_provider(
                UserPoolId=pool_id,
                ProviderName="UpdOIDC",
                ProviderDetails={
                    "client_id": "new-client",
                    "authorize_scopes": "openid email",
                    "oidc_issuer": "https://example.com",
                },
            )
            assert resp["IdentityProvider"]["ProviderDetails"]["client_id"] == "new-client"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_identity_provider(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("delidp-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_identity_provider(
                UserPoolId=pool_id,
                ProviderName="DelOIDC",
                ProviderType="OIDC",
                ProviderDetails={
                    "client_id": "test",
                    "authorize_scopes": "openid",
                    "oidc_issuer": "https://example.com",
                },
            )
            cognito.delete_identity_provider(UserPoolId=pool_id, ProviderName="DelOIDC")
            resp = cognito.list_identity_providers(UserPoolId=pool_id, MaxResults=10)
            names = [p["ProviderName"] for p in resp["Providers"]]
            assert "DelOIDC" not in names
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoResourceServers:
    """Tests for resource server operations."""

    def test_create_resource_server(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("rs-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            resp = cognito.create_resource_server(
                UserPoolId=pool_id,
                Identifier="https://api.example.com",
                Name="Test API",
                Scopes=[{"ScopeName": "read", "ScopeDescription": "Read access"}],
            )
            assert resp["ResourceServer"]["Identifier"] == "https://api.example.com"
            assert resp["ResourceServer"]["Name"] == "Test API"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_resource_server(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("drs-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            cognito.create_resource_server(
                UserPoolId=pool_id,
                Identifier="https://api2.example.com",
                Name="API 2",
            )
            resp = cognito.describe_resource_server(
                UserPoolId=pool_id, Identifier="https://api2.example.com"
            )
            assert resp["ResourceServer"]["Name"] == "API 2"
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoUserPoolDomains:
    """Tests for user pool domain operations."""

    def test_create_user_pool_domain(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("dom-pool"))["UserPool"]
        pool_id = pool["Id"]
        domain = f"test-domain-{uuid.uuid4().hex[:8]}"
        try:
            resp = cognito.create_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                cognito.delete_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            except Exception:
                pass  # best-effort cleanup
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_describe_user_pool_domain_exists(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("ddom-pool"))["UserPool"]
        pool_id = pool["Id"]
        domain = f"test-domain-{uuid.uuid4().hex[:8]}"
        try:
            cognito.create_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            resp = cognito.describe_user_pool_domain(Domain=domain)
            assert "DomainDescription" in resp
            assert resp["DomainDescription"].get("Domain") == domain
        finally:
            try:
                cognito.delete_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            except Exception:
                pass  # best-effort cleanup
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_delete_user_pool_domain(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("deldom-pool"))["UserPool"]
        pool_id = pool["Id"]
        domain = f"test-domain-{uuid.uuid4().hex[:8]}"
        try:
            cognito.create_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            resp = cognito.delete_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoAdminUserExtended:
    """Tests for additional admin user operations."""

    def test_admin_delete_user_attributes(self, cognito):
        pool = cognito.create_user_pool(
            PoolName=_unique("delattr-pool"),
            Schema=[{"Name": "email", "AttributeDataType": "String", "Mutable": True}],
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("delattr-user")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
                UserAttributes=[{"Name": "email", "Value": "test@example.com"}],
            )
            resp = cognito.admin_delete_user_attributes(
                UserPoolId=pool_id,
                Username=username,
                UserAttributeNames=["email"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_reset_user_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("rstpw-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("rstpw-user")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            resp = cognito.admin_reset_user_password(UserPoolId=pool_id, Username=username)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_set_user_mfa_preference(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("mfapref-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("mfapref-user")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            resp = cognito.admin_set_user_mfa_preference(
                UserPoolId=pool_id,
                Username=username,
                SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_set_user_password(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("setpw-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("setpw-user")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            resp = cognito.admin_set_user_password(
                UserPoolId=pool_id,
                Username=username,
                Password="NewPermanent1!",
                Permanent=True,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_admin_user_global_sign_out(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("gsignout-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            username = _unique("gsignout-user")
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            resp = cognito.admin_user_global_sign_out(UserPoolId=pool_id, Username=username)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)


class TestCognitoAuthExtended:
    """Tests for additional auth flow operations."""

    def test_confirm_sign_up(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("csignup-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client_resp = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName="csignup-client",
                ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
            )
            client_id = client_resp["UserPoolClient"]["ClientId"]
            username = _unique("csignup-user")
            cognito.sign_up(
                ClientId=client_id,
                Username=username,
                Password="Test@12345678",
            )
            resp = cognito.confirm_sign_up(
                ClientId=client_id,
                Username=username,
                ConfirmationCode="123456",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_global_sign_out(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("gso-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client_resp = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName="gso-client",
                ExplicitAuthFlows=[
                    "ALLOW_USER_PASSWORD_AUTH",
                    "ALLOW_REFRESH_TOKEN_AUTH",
                ],
            )
            client_id = client_resp["UserPoolClient"]["ClientId"]
            username = _unique("gso-user")
            cognito.sign_up(
                ClientId=client_id,
                Username=username,
                Password="Test@12345678",
            )
            cognito.admin_confirm_sign_up(UserPoolId=pool_id, Username=username)
            auth = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": "Test@12345678"},
            )
            access_token = auth["AuthenticationResult"]["AccessToken"]
            resp = cognito.global_sign_out(AccessToken=access_token)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_attributes(self, cognito):
        pool = cognito.create_user_pool(
            PoolName=_unique("updattr-pool"),
            Schema=[{"Name": "email", "AttributeDataType": "String", "Mutable": True}],
            AutoVerifiedAttributes=["email"],
        )["UserPool"]
        pool_id = pool["Id"]
        try:
            client_resp = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName="updattr-client",
                ExplicitAuthFlows=[
                    "ALLOW_USER_PASSWORD_AUTH",
                    "ALLOW_REFRESH_TOKEN_AUTH",
                ],
            )
            client_id = client_resp["UserPoolClient"]["ClientId"]
            username = _unique("updattr-user")
            cognito.sign_up(
                ClientId=client_id,
                Username=username,
                Password="Test@12345678",
                UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
            )
            cognito.admin_confirm_sign_up(UserPoolId=pool_id, Username=username)
            auth = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": "Test@12345678"},
            )
            access_token = auth["AuthenticationResult"]["AccessToken"]
            resp = cognito.update_user_attributes(
                AccessToken=access_token,
                UserAttributes=[{"Name": "email", "Value": "new@example.com"}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_update_user_pool_domain(self, cognito):
        pool = cognito.create_user_pool(PoolName=_unique("upddom-pool"))["UserPool"]
        pool_id = pool["Id"]
        domain = f"test-upd-domain-{uuid.uuid4().hex[:8]}"
        try:
            cognito.create_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            resp = cognito.update_user_pool_domain(
                UserPoolId=pool_id,
                Domain=domain,
                CustomDomainConfig={
                    "CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/fake"
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                cognito.delete_user_pool_domain(UserPoolId=pool_id, Domain=domain)
            except Exception:
                pass  # best-effort cleanup
            cognito.delete_user_pool(UserPoolId=pool_id)

    def test_respond_to_auth_challenge(self, cognito):
        """RespondToAuthChallenge with NEW_PASSWORD_REQUIRED challenge."""
        pool = cognito.create_user_pool(PoolName=_unique("rtac-pool"))["UserPool"]
        pool_id = pool["Id"]
        try:
            client_resp = cognito.create_user_pool_client(
                UserPoolId=pool_id,
                ClientName="rtac-client",
                ExplicitAuthFlows=[
                    "ALLOW_USER_PASSWORD_AUTH",
                    "ALLOW_REFRESH_TOKEN_AUTH",
                ],
            )
            client_id = client_resp["UserPoolClient"]["ClientId"]
            username = _unique("rtac-user")
            # Admin-created users get a FORCE_CHANGE_PASSWORD status
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username=username,
                TemporaryPassword="TempPass1!",
                MessageAction="SUPPRESS",
            )
            # This will return a NEW_PASSWORD_REQUIRED challenge
            auth = cognito.admin_initiate_auth(
                UserPoolId=pool_id,
                ClientId=client_id,
                AuthFlow="ADMIN_USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": "TempPass1!"},
            )
            if "ChallengeName" in auth and auth["ChallengeName"] == "NEW_PASSWORD_REQUIRED":
                resp = cognito.respond_to_auth_challenge(
                    ClientId=client_id,
                    ChallengeName="NEW_PASSWORD_REQUIRED",
                    Session=auth["Session"],
                    ChallengeResponses={
                        "USERNAME": username,
                        "NEW_PASSWORD": "NewPermanent1!",
                    },
                )
                assert "AuthenticationResult" in resp
            else:
                assert "AuthenticationResult" in auth
        finally:
            cognito.delete_user_pool(UserPoolId=pool_id)
