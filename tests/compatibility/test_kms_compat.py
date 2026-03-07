"""KMS compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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

    def test_create_key_with_tags(self, kms):
        uid = uuid.uuid4().hex[:8]
        key = kms.create_key(
            Description=f"tagged-{uid}",
            Tags=[
                {"TagKey": "project", "TagValue": "robotocore"},
                {"TagKey": "tier", "TagValue": "free"},
            ],
        )
        key_id = key["KeyMetadata"]["KeyId"]
        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map["project"] == "robotocore"
        assert tag_map["tier"] == "free"
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_tag_resource(self, kms):
        key = kms.create_key(Description="tag-resource-test")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.tag_resource(
            KeyId=key_id, Tags=[{"TagKey": "added", "TagValue": "later"}]
        )
        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map["added"] == "later"
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_untag_resource(self, kms):
        key = kms.create_key(
            Description="untag-test",
            Tags=[
                {"TagKey": "keep", "TagValue": "yes"},
                {"TagKey": "remove", "TagValue": "bye"},
            ],
        )
        key_id = key["KeyMetadata"]["KeyId"]
        kms.untag_resource(KeyId=key_id, TagKeys=["remove"])
        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert "remove" not in tag_map
        assert tag_map["keep"] == "yes"
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_create_list_delete_alias(self, kms):
        uid = uuid.uuid4().hex[:8]
        alias_name = f"alias/test-{uid}"
        key = kms.create_key(Description=f"alias-cld-{uid}")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName=alias_name, TargetKeyId=key_id)

        aliases = kms.list_aliases()
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert alias_name in alias_names

        kms.delete_alias(AliasName=alias_name)
        aliases = kms.list_aliases()
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert alias_name not in alias_names
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_update_alias(self, kms):
        uid = uuid.uuid4().hex[:8]
        alias_name = f"alias/upd-{uid}"
        key1 = kms.create_key(Description=f"alias-upd1-{uid}")
        key2 = kms.create_key(Description=f"alias-upd2-{uid}")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2_id = key2["KeyMetadata"]["KeyId"]

        kms.create_alias(AliasName=alias_name, TargetKeyId=key1_id)
        kms.update_alias(AliasName=alias_name, TargetKeyId=key2_id)

        aliases = kms.list_aliases(KeyId=key2_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert alias_name in alias_names

        kms.delete_alias(AliasName=alias_name)
        kms.schedule_key_deletion(KeyId=key1_id, PendingWindowInDays=7)
        kms.schedule_key_deletion(KeyId=key2_id, PendingWindowInDays=7)

    def test_enable_key_rotation(self, kms):
        key = kms.create_key(Description="rotation-test")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.enable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is True

        kms.disable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is False

        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_generate_data_key_without_plaintext(self, kms):
        key = kms.create_key(Description="dk-no-pt")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.generate_data_key_without_plaintext(
            KeyId=key_id, KeySpec="AES_256"
        )
        assert "CiphertextBlob" in response
        assert "Plaintext" not in response
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_re_encrypt(self, kms):
        key1 = kms.create_key(Description="reenc-src")
        key2 = kms.create_key(Description="reenc-dst")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2_id = key2["KeyMetadata"]["KeyId"]

        encrypted = kms.encrypt(KeyId=key1_id, Plaintext=b"reencrypt me")
        re_encrypted = kms.re_encrypt(
            CiphertextBlob=encrypted["CiphertextBlob"],
            DestinationKeyId=key2_id,
        )
        decrypted = kms.decrypt(CiphertextBlob=re_encrypted["CiphertextBlob"])
        assert decrypted["Plaintext"] == b"reencrypt me"

        kms.schedule_key_deletion(KeyId=key1_id, PendingWindowInDays=7)
        kms.schedule_key_deletion(KeyId=key2_id, PendingWindowInDays=7)

    def test_describe_key_metadata_fields(self, kms):
        key = kms.create_key(Description="metadata-test")
        key_id = key["KeyMetadata"]["KeyId"]

        desc = kms.describe_key(KeyId=key_id)
        meta = desc["KeyMetadata"]
        assert meta["KeyId"] == key_id
        assert "Arn" in meta
        assert meta["KeyState"] == "Enabled"
        assert "CreationDate" in meta
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_schedule_and_cancel_key_deletion(self, kms):
        key = kms.create_key(Description="del-cancel-test")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["KeyState"] == "PendingDeletion"

        kms.cancel_key_deletion(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["KeyState"] == "Disabled"
        # Re-enable and clean up
        kms.enable_key(KeyId=key_id)
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_list_aliases_for_key(self, kms):
        uid = uuid.uuid4().hex[:8]
        key = kms.create_key(Description=f"alias-list-{uid}")
        key_id = key["KeyMetadata"]["KeyId"]
        alias1 = f"alias/lk1-{uid}"
        alias2 = f"alias/lk2-{uid}"
        kms.create_alias(AliasName=alias1, TargetKeyId=key_id)
        kms.create_alias(AliasName=alias2, TargetKeyId=key_id)

        aliases = kms.list_aliases(KeyId=key_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert alias1 in alias_names
        assert alias2 in alias_names

        kms.delete_alias(AliasName=alias1)
        kms.delete_alias(AliasName=alias2)
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_encrypt_decrypt_with_context(self, kms):
        key = kms.create_key(Description="ctx-test")
        key_id = key["KeyMetadata"]["KeyId"]
        ctx = {"purpose": "test"}

        encrypted = kms.encrypt(
            KeyId=key_id, Plaintext=b"context data", EncryptionContext=ctx
        )
        decrypted = kms.decrypt(
            CiphertextBlob=encrypted["CiphertextBlob"], EncryptionContext=ctx
        )
        assert decrypted["Plaintext"] == b"context data"
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_decrypt_wrong_context_fails(self, kms):
        key = kms.create_key(Description="bad-ctx-test")
        key_id = key["KeyMetadata"]["KeyId"]

        encrypted = kms.encrypt(
            KeyId=key_id, Plaintext=b"data", EncryptionContext={"a": "b"}
        )
        with pytest.raises(ClientError) as exc_info:
            kms.decrypt(
                CiphertextBlob=encrypted["CiphertextBlob"],
                EncryptionContext={"wrong": "context"},
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "InvalidCiphertextException",
            "IncorrectKeyException",
        )
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_generate_random(self, kms):
        response = kms.generate_random(NumberOfBytes=32)
        assert len(response["Plaintext"]) == 32

    def test_create_key_symmetric_default(self, kms):
        key = kms.create_key(Description="sym-default")
        meta = key["KeyMetadata"]
        assert meta["KeySpec"] in ("SYMMETRIC_DEFAULT", "AES_256")
        kms.schedule_key_deletion(KeyId=meta["KeyId"], PendingWindowInDays=7)

    def test_list_keys_contains_created_key(self, kms):
        key = kms.create_key(Description="list-check")
        key_id = key["KeyMetadata"]["KeyId"]
        response = kms.list_keys()
        key_ids = [k["KeyId"] for k in response["Keys"]]
        assert key_id in key_ids
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
