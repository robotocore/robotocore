"""KMS compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def kms():
    return make_client("kms")


class TestKMSOperations:
    def test_create_key(self, kms):
        response = kms.create_key(Description="test key")
        assert "KeyMetadata" in response
        key_id = response["KeyMetadata"]["KeyId"]
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_list_keys(self, kms):
        kms.create_key(Description="list key")
        response = kms.list_keys()
        assert len(response["Keys"]) >= 1

    def test_encrypt_decrypt(self, kms):
        key = kms.create_key(Description="encrypt key")
        key_id = key["KeyMetadata"]["KeyId"]

        encrypted = kms.encrypt(KeyId=key_id, Plaintext=b"secret data")
        assert "CiphertextBlob" in encrypted

        decrypted = kms.decrypt(CiphertextBlob=encrypted["CiphertextBlob"])
        assert decrypted["Plaintext"] == b"secret data"

    def test_generate_data_key(self, kms):
        key = kms.create_key(Description="data key test")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.generate_data_key(KeyId=key_id, KeySpec="AES_256")
        assert "Plaintext" in response
        assert "CiphertextBlob" in response

    def test_describe_key(self, kms):
        key = kms.create_key(Description="describe key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.describe_key(KeyId=key_id)
        assert response["KeyMetadata"]["Description"] == "describe key"

    def test_create_alias(self, kms):
        key = kms.create_key(Description="alias key")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName="alias/test-alias", TargetKeyId=key_id)

        aliases = kms.list_aliases()
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert "alias/test-alias" in alias_names
        kms.delete_alias(AliasName="alias/test-alias")

    def test_enable_disable_key(self, kms):
        key = kms.create_key(Description="toggle key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.disable_key(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["Enabled"] is False

        kms.enable_key(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["Enabled"] is True

    def test_encrypt_with_alias(self, kms):
        key = kms.create_key(Description="alias encrypt")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName="alias/enc-test", TargetKeyId=key_id)

        encrypted = kms.encrypt(KeyId="alias/enc-test", Plaintext=b"test data")
        decrypted = kms.decrypt(CiphertextBlob=encrypted["CiphertextBlob"])
        assert decrypted["Plaintext"] == b"test data"
        kms.delete_alias(AliasName="alias/enc-test")

    def test_tag_key(self, kms):
        key = kms.create_key(Description="tag key", Tags=[{"TagKey": "env", "TagValue": "test"}])
        key_id = key["KeyMetadata"]["KeyId"]

        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map.get("env") == "test"

    def test_create_key_symmetric_default(self, kms):
        """Create a key with SYMMETRIC_DEFAULT spec."""
        response = kms.create_key(
            Description="symmetric key",
            KeySpec="SYMMETRIC_DEFAULT",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        meta = response["KeyMetadata"]
        assert meta["KeySpec"] == "SYMMETRIC_DEFAULT"
        assert meta["KeyUsage"] == "ENCRYPT_DECRYPT"
        assert meta["KeyManager"] == "CUSTOMER"
        kms.schedule_key_deletion(KeyId=meta["KeyId"], PendingWindowInDays=7)

    def test_create_key_rsa_2048(self, kms):
        """Create an asymmetric RSA key."""
        response = kms.create_key(
            Description="rsa key",
            KeySpec="RSA_2048",
            KeyUsage="SIGN_VERIFY",
        )
        meta = response["KeyMetadata"]
        assert meta["KeySpec"] == "RSA_2048"
        assert meta["KeyUsage"] == "SIGN_VERIFY"
        kms.schedule_key_deletion(KeyId=meta["KeyId"], PendingWindowInDays=7)

    def test_list_keys_pagination(self, kms):
        """List keys returns truncated results with marker."""
        key_ids = []
        for i in range(3):
            resp = kms.create_key(Description=f"list-page-key-{i}")
            key_ids.append(resp["KeyMetadata"]["KeyId"])

        response = kms.list_keys(Limit=1)
        assert len(response["Keys"]) >= 1
        assert "Truncated" in response

    def test_describe_key_arn(self, kms):
        """Describe key using ARN instead of key ID."""
        key = kms.create_key(Description="arn-describe key")
        key_arn = key["KeyMetadata"]["Arn"]

        response = kms.describe_key(KeyId=key_arn)
        assert response["KeyMetadata"]["Arn"] == key_arn
        assert response["KeyMetadata"]["Description"] == "arn-describe key"

    def test_create_and_list_aliases(self, kms):
        """Create multiple aliases and verify listing."""
        key = kms.create_key(Description="multi-alias key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.create_alias(AliasName="alias/compat-alias-1", TargetKeyId=key_id)
        kms.create_alias(AliasName="alias/compat-alias-2", TargetKeyId=key_id)

        aliases = kms.list_aliases(KeyId=key_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert "alias/compat-alias-1" in alias_names
        assert "alias/compat-alias-2" in alias_names

        kms.delete_alias(AliasName="alias/compat-alias-1")
        kms.delete_alias(AliasName="alias/compat-alias-2")

    def test_delete_alias(self, kms):
        """Delete alias and verify it is gone."""
        key = kms.create_key(Description="del-alias key")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName="alias/to-delete", TargetKeyId=key_id)

        kms.delete_alias(AliasName="alias/to-delete")

        aliases = kms.list_aliases(KeyId=key_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert "alias/to-delete" not in alias_names

    def test_encrypt_decrypt_with_context(self, kms):
        """Encrypt/decrypt with encryption context."""
        key = kms.create_key(Description="context-enc key")
        key_id = key["KeyMetadata"]["KeyId"]
        context = {"purpose": "testing", "app": "robotocore"}

        encrypted = kms.encrypt(
            KeyId=key_id,
            Plaintext=b"context secret",
            EncryptionContext=context,
        )
        decrypted = kms.decrypt(
            CiphertextBlob=encrypted["CiphertextBlob"],
            EncryptionContext=context,
        )
        assert decrypted["Plaintext"] == b"context secret"

    def test_generate_data_key_without_plaintext(self, kms):
        """Generate data key without plaintext returns only ciphertext."""
        key = kms.create_key(Description="datakeynoplain key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.generate_data_key_without_plaintext(KeyId=key_id, KeySpec="AES_256")
        assert "CiphertextBlob" in response
        assert "Plaintext" not in response or response.get("Plaintext") is None

    def test_tag_untag_list_tags(self, kms):
        """Tag, untag, and list resource tags on a key."""
        key = kms.create_key(Description="tag-ops key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.tag_resource(
            KeyId=key_id,
            Tags=[
                {"TagKey": "team", "TagValue": "platform"},
                {"TagKey": "cost-center", "TagValue": "12345"},
            ],
        )

        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map["team"] == "platform"
        assert tag_map["cost-center"] == "12345"

        kms.untag_resource(KeyId=key_id, TagKeys=["cost-center"])

        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert "cost-center" not in tag_map
        assert tag_map["team"] == "platform"

    def test_enable_key_rotation(self, kms):
        """Enable key rotation and verify status."""
        key = kms.create_key(Description="rotation key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.enable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is True

    def test_disable_key_rotation(self, kms):
        """Enable then disable key rotation."""
        key = kms.create_key(Description="rotation-off key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.enable_key_rotation(KeyId=key_id)
        kms.disable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is False

    def test_get_key_policy(self, kms):
        """Get the default key policy."""
        key = kms.create_key(Description="policy key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.get_key_policy(KeyId=key_id, PolicyName="default")
        assert "Policy" in response
        assert len(response["Policy"]) > 0

    def test_list_key_policies(self, kms):
        """List key policies returns default policy name."""
        key = kms.create_key(Description="list-policy key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.list_key_policies(KeyId=key_id)
        assert "default" in response["PolicyNames"]

    def test_put_key_policy(self, kms):
        """Put a key policy and verify it can be retrieved."""
        key = kms.create_key(Description="put-policy key")
        key_id = key["KeyMetadata"]["KeyId"]

        policy = (
            '{"Version":"2012-10-17","Id":"custom-policy",'
            '"Statement":[{"Sid":"EnableRootAccess","Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"kms:*","Resource":"*"}]}'
        )

        kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=policy)
        response = kms.get_key_policy(KeyId=key_id, PolicyName="default")
        assert "custom-policy" in response["Policy"]
