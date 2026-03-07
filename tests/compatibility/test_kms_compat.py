"""KMS compatibility tests."""

import json

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

    def test_get_key_rotation_status(self, kms):
        key = kms.create_key(Description="rotation status key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.get_key_rotation_status(KeyId=key_id)
        assert response["KeyRotationEnabled"] is False

        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_key_enable_disable(self, kms):
        key = kms.create_key(Description="enable disable key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.disable_key(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["Enabled"] is False

        kms.enable_key(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["Enabled"] is True

        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_tag_key(self, kms):
        key = kms.create_key(Description="tag key", Tags=[{"TagKey": "env", "TagValue": "test"}])
        key_id = key["KeyMetadata"]["KeyId"]

        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map.get("env") == "test"

    def test_enable_key_rotation(self, kms):
        key = kms.create_key(Description="rotation key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.enable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is True

        kms.disable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is False

    def test_list_aliases_for_key(self, kms):
        key = kms.create_key(Description="alias list key")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName="alias/list-test-1", TargetKeyId=key_id)
        kms.create_alias(AliasName="alias/list-test-2", TargetKeyId=key_id)

        aliases = kms.list_aliases(KeyId=key_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert "alias/list-test-1" in alias_names
        assert "alias/list-test-2" in alias_names

        kms.delete_alias(AliasName="alias/list-test-1")
        kms.delete_alias(AliasName="alias/list-test-2")

    def test_update_alias(self, kms):
        key1 = kms.create_key(Description="alias update key 1")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2 = kms.create_key(Description="alias update key 2")
        key2_id = key2["KeyMetadata"]["KeyId"]

        kms.create_alias(AliasName="alias/update-test", TargetKeyId=key1_id)
        kms.update_alias(AliasName="alias/update-test", TargetKeyId=key2_id)

        aliases = kms.list_aliases(KeyId=key2_id)
        alias_names = [a["AliasName"] for a in aliases["Aliases"]]
        assert "alias/update-test" in alias_names

        kms.delete_alias(AliasName="alias/update-test")

    def test_tag_untag_resource(self, kms):
        key = kms.create_key(Description="tag untag key")
        key_id = key["KeyMetadata"]["KeyId"]

        kms.tag_resource(
            KeyId=key_id,
            Tags=[
                {"TagKey": "team", "TagValue": "platform"},
                {"TagKey": "cost", "TagValue": "dev"},
            ],
        )
        tags = kms.list_resource_tags(KeyId=key_id)
        tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
        assert tag_map["team"] == "platform"
        assert tag_map["cost"] == "dev"

        kms.untag_resource(KeyId=key_id, TagKeys=["cost"])
        tags = kms.list_resource_tags(KeyId=key_id)
        tag_keys = [t["TagKey"] for t in tags["Tags"]]
        assert "team" in tag_keys
        assert "cost" not in tag_keys

    def test_get_key_policy(self, kms):
        key = kms.create_key(Description="policy key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.get_key_policy(KeyId=key_id, PolicyName="default")
        assert response["Policy"] is not None

    def test_list_key_policies(self, kms):
        key = kms.create_key(Description="list policy key")
        key_id = key["KeyMetadata"]["KeyId"]

        response = kms.list_key_policies(KeyId=key_id)
        assert "default" in response["PolicyNames"]

    def test_encrypt_decrypt_with_context(self, kms):
        key = kms.create_key(Description="context enc key")
        key_id = key["KeyMetadata"]["KeyId"]
        context = {"purpose": "testing", "env": "dev"}

        encrypted = kms.encrypt(
            KeyId=key_id, Plaintext=b"context secret", EncryptionContext=context
        )
        decrypted = kms.decrypt(
            CiphertextBlob=encrypted["CiphertextBlob"], EncryptionContext=context
        )
        assert decrypted["Plaintext"] == b"context secret"

    def test_generate_data_key_without_plaintext(self, kms):
        key = kms.create_key(Description="data key no pt")
    def test_re_encrypt(self, kms):
        key1 = kms.create_key(Description="reencrypt src")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2 = kms.create_key(Description="reencrypt dst")
        key2_id = key2["KeyMetadata"]["KeyId"]

        encrypted = kms.encrypt(KeyId=key1_id, Plaintext=b"reencrypt me")
        re_encrypted = kms.re_encrypt(
            CiphertextBlob=encrypted["CiphertextBlob"],
            DestinationKeyId=key2_id,
        )
        assert "CiphertextBlob" in re_encrypted

        decrypted = kms.decrypt(CiphertextBlob=re_encrypted["CiphertextBlob"])
        assert decrypted["Plaintext"] == b"reencrypt me"

    def test_describe_key_with_alias(self, kms):
        key = kms.create_key(Description="alias desc key")
        key_id = key["KeyMetadata"]["KeyId"]
        kms.create_alias(AliasName="alias/desc-test", TargetKeyId=key_id)

        response = kms.describe_key(KeyId="alias/desc-test")
        assert response["KeyMetadata"]["KeyId"] == key_id
        assert response["KeyMetadata"]["Description"] == "alias desc key"

        kms.delete_alias(AliasName="alias/desc-test")

    def test_schedule_and_cancel_key_deletion(self, kms):
        key = kms.create_key(Description="delete cancel key")
        key_id = key["KeyMetadata"]["KeyId"]

        resp = kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
        assert "DeletionDate" in resp

        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["KeyState"] == "PendingDeletion"

        kms.cancel_key_deletion(KeyId=key_id)
        desc = kms.describe_key(KeyId=key_id)
        assert desc["KeyMetadata"]["KeyState"] in ("Disabled", "Enabled", "PendingImport")

    def test_create_grant(self, kms):
        key = kms.create_key(Description="grant key")
        key_id = key["KeyMetadata"]["KeyId"]

        grant = kms.create_grant(
            KeyId=key_id,
            GranteePrincipal="arn:aws:iam::123456789012:root",
            Operations=["Encrypt", "Decrypt"],
        )
        assert "GrantId" in grant
        assert "GrantToken" in grant

        grants = kms.list_grants(KeyId=key_id)
        grant_ids = [g["GrantId"] for g in grants["Grants"]]
        assert grant["GrantId"] in grant_ids

        kms.revoke_grant(KeyId=key_id, GrantId=grant["GrantId"])
        grants = kms.list_grants(KeyId=key_id)
        grant_ids = [g["GrantId"] for g in grants["Grants"]]
        assert grant["GrantId"] not in grant_ids
        """ReEncrypt re-encrypts ciphertext under a different key."""
        key1 = kms.create_key(Description="re-encrypt-src")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2 = kms.create_key(Description="re-encrypt-dst")
        key2_id = key2["KeyMetadata"]["KeyId"]

        # Encrypt under key1
        enc = kms.encrypt(KeyId=key1_id, Plaintext=b"reencrypt me")
        ct1 = enc["CiphertextBlob"]

        # Re-encrypt under key2
        re_enc = kms.re_encrypt(
            CiphertextBlob=ct1,
            DestinationKeyId=key2_id,
        )
        ct2 = re_enc["CiphertextBlob"]
        assert ct2 != ct1  # ciphertext should differ

        # Decrypt should yield original plaintext
        dec = kms.decrypt(CiphertextBlob=ct2)
        assert dec["Plaintext"] == b"reencrypt me"

    def test_put_key_policy(self, kms):
        """PutKeyPolicy sets a key policy and GetKeyPolicy retrieves it."""
        key = kms.create_key(Description="policy-put")
        key_id = key["KeyMetadata"]["KeyId"]

        import json
        policy = json.dumps({
            "Version": "2012-10-17",
            "Id": "custom-policy",
            "Statement": [
                {
                    "Sid": "Enable IAM User Permissions",
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": "kms:*",
                    "Resource": "*",
                }
            ],
        })
        kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=policy)

        response = kms.get_key_policy(KeyId=key_id, PolicyName="default")
        retrieved = json.loads(response["Policy"])
        assert retrieved["Id"] == "custom-policy"

    def test_key_rotation_status(self, kms):
        """Enable, check, and disable key rotation."""
        key = kms.create_key(Description="rotation-test")
        key_id = key["KeyMetadata"]["KeyId"]

        # Default: rotation disabled
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is False

        # Enable rotation
        kms.enable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is True

        # Disable rotation
        kms.disable_key_rotation(KeyId=key_id)
        status = kms.get_key_rotation_status(KeyId=key_id)
        assert status["KeyRotationEnabled"] is False

    def test_generate_random(self, kms):
        """GenerateRandom returns random bytes of specified length."""
        response = kms.generate_random(NumberOfBytes=32)
        assert "Plaintext" in response
        assert len(response["Plaintext"]) == 32

        # Different calls should (almost certainly) produce different bytes
        response2 = kms.generate_random(NumberOfBytes=32)
        assert response["Plaintext"] != response2["Plaintext"]

    def test_create_key_symmetric_default(self, kms):
        """Test CreateKey with SYMMETRIC_DEFAULT key spec."""
        response = kms.create_key(
            Description="symmetric key",
            KeySpec="SYMMETRIC_DEFAULT",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        meta = response["KeyMetadata"]
        assert meta["KeySpec"] == "SYMMETRIC_DEFAULT"
        assert meta["KeyUsage"] == "ENCRYPT_DECRYPT"
        kms.schedule_key_deletion(KeyId=meta["KeyId"], PendingWindowInDays=7)

    def test_create_key_rsa_2048(self, kms):
        """Test CreateKey with RSA_2048 key spec."""
        response = kms.create_key(
            Description="rsa key",
            KeySpec="RSA_2048",
            KeyUsage="SIGN_VERIFY",
        )
        meta = response["KeyMetadata"]
        assert meta["KeySpec"] == "RSA_2048"
        assert meta["KeyUsage"] == "SIGN_VERIFY"
        kms.schedule_key_deletion(KeyId=meta["KeyId"], PendingWindowInDays=7)

    def test_describe_key_all_fields(self, kms):
        """Test DescribeKey returns all expected fields."""
        key = kms.create_key(Description="describe all fields")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            response = kms.describe_key(KeyId=key_id)
            meta = response["KeyMetadata"]
            assert "KeyId" in meta
            assert "Arn" in meta
            assert "CreationDate" in meta
            assert "Enabled" in meta
            assert "Description" in meta
            assert "KeyState" in meta
            assert "KeyUsage" in meta or "CustomerMasterKeySpec" in meta
            assert "KeyManager" in meta
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_create_update_delete_alias(self, kms):
        """Test CreateAlias, ListAliases, UpdateAlias, DeleteAlias."""
        key1 = kms.create_key(Description="alias key 1")
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2 = kms.create_key(Description="alias key 2")
        key2_id = key2["KeyMetadata"]["KeyId"]
        alias_name = "alias/compat-alias-crud"
        try:
            kms.create_alias(AliasName=alias_name, TargetKeyId=key1_id)
            aliases = kms.list_aliases()
            alias_names = [a["AliasName"] for a in aliases["Aliases"]]
            assert alias_name in alias_names

            # Find target key
            alias_entry = next(a for a in aliases["Aliases"] if a["AliasName"] == alias_name)
            assert alias_entry["TargetKeyId"] == key1_id

            # Update alias to point to key2
            kms.update_alias(AliasName=alias_name, TargetKeyId=key2_id)
            aliases2 = kms.list_aliases()
            alias_entry2 = next(a for a in aliases2["Aliases"] if a["AliasName"] == alias_name)
            assert alias_entry2["TargetKeyId"] == key2_id

            kms.delete_alias(AliasName=alias_name)
            aliases3 = kms.list_aliases()
            alias_names3 = [a["AliasName"] for a in aliases3["Aliases"]]
            assert alias_name not in alias_names3
        finally:
            kms.schedule_key_deletion(KeyId=key1_id, PendingWindowInDays=7)
            kms.schedule_key_deletion(KeyId=key2_id, PendingWindowInDays=7)

    def test_encrypt_decrypt_roundtrip(self, kms):
        """Test Encrypt/Decrypt round-trip with various plaintext."""
        key = kms.create_key(Description="roundtrip key")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            for plaintext in [b"hello", b"x" * 4096, b"\x00\x01\x02\xff"]:
                encrypted = kms.encrypt(KeyId=key_id, Plaintext=plaintext)
                decrypted = kms.decrypt(CiphertextBlob=encrypted["CiphertextBlob"])
                assert decrypted["Plaintext"] == plaintext
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_enable_disable_key_rotation(self, kms):
        """Test EnableKeyRotation, GetKeyRotationStatus, DisableKeyRotation."""
        key = kms.create_key(Description="rotation key")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            status = kms.get_key_rotation_status(KeyId=key_id)
            assert status["KeyRotationEnabled"] is False

            kms.enable_key_rotation(KeyId=key_id)
            status2 = kms.get_key_rotation_status(KeyId=key_id)
            assert status2["KeyRotationEnabled"] is True

            kms.disable_key_rotation(KeyId=key_id)
            status3 = kms.get_key_rotation_status(KeyId=key_id)
            assert status3["KeyRotationEnabled"] is False
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_enable_disable_key_state(self, kms):
        """Test EnableKey and DisableKey affect KeyState."""
        key = kms.create_key(Description="state key")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            kms.disable_key(KeyId=key_id)
            desc = kms.describe_key(KeyId=key_id)
            assert desc["KeyMetadata"]["KeyState"] == "Disabled"

            kms.enable_key(KeyId=key_id)
            desc2 = kms.describe_key(KeyId=key_id)
            assert desc2["KeyMetadata"]["KeyState"] == "Enabled"
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_tag_untag_list_resource_tags(self, kms):
        """Test TagResource, UntagResource, ListResourceTags."""
        key = kms.create_key(Description="tag crud key")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            kms.tag_resource(
                KeyId=key_id,
                Tags=[
                    {"TagKey": "project", "TagValue": "robotocore"},
                    {"TagKey": "team", "TagValue": "platform"},
                ],
            )
            tags = kms.list_resource_tags(KeyId=key_id)
            tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
            assert tag_map["project"] == "robotocore"
            assert tag_map["team"] == "platform"

            kms.untag_resource(KeyId=key_id, TagKeys=["team"])
            tags2 = kms.list_resource_tags(KeyId=key_id)
            tag_map2 = {t["TagKey"]: t["TagValue"] for t in tags2["Tags"]}
            assert "team" not in tag_map2
            assert tag_map2["project"] == "robotocore"
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
    def test_sign_verify_rsa(self, kms):
        """Test Sign and Verify with an RSA key."""
        key = kms.create_key(
            Description="sign verify key",
            KeySpec="RSA_2048",
            KeyUsage="SIGN_VERIFY",
        )
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            message = b"message to sign"
            sign_resp = kms.sign(
                KeyId=key_id,
                Message=message,
                MessageType="RAW",
                SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
            )
            assert "Signature" in sign_resp

            verify_resp = kms.verify(
                KeyId=key_id,
                Message=message,
                MessageType="RAW",
                Signature=sign_resp["Signature"],
                SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
            )
            assert verify_resp["SignatureValid"] is True
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_update_key_description(self, kms):
        """Test UpdateKeyDescription."""
        key = kms.create_key(Description="original description")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            kms.update_key_description(KeyId=key_id, Description="updated description")
            desc = kms.describe_key(KeyId=key_id)
            assert desc["KeyMetadata"]["Description"] == "updated description"
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_get_put_key_policy(self, kms):
        """Test GetKeyPolicy and PutKeyPolicy."""
        key = kms.create_key(Description="policy key")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            get_resp = kms.get_key_policy(KeyId=key_id, PolicyName="default")
            assert "Policy" in get_resp
            policy = get_resp["Policy"]
            # Policy should be valid JSON
            parsed = json.loads(policy) if isinstance(policy, str) else policy
            assert "Statement" in parsed

            # Put a new policy
            new_policy = json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Enable IAM policies",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "kms:*",
                        "Resource": "*",
                    }
                ],
            })
            kms.put_key_policy(KeyId=key_id, PolicyName="default", Policy=new_policy)
            get_resp2 = kms.get_key_policy(KeyId=key_id, PolicyName="default")
            policy2 = get_resp2["Policy"]
            parsed2 = json.loads(policy2) if isinstance(policy2, str) else policy2
            assert parsed2["Statement"][0]["Sid"] == "Enable IAM policies"
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


class TestKMSExtended:
    """Extended KMS operations for higher coverage."""

    @pytest.fixture
    def kms(self):
        from tests.compatibility.conftest import make_client
        return make_client("kms")

    def test_generate_data_key(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            resp = kms.generate_data_key(KeyId=key_id, KeySpec="AES_256")
            assert "Plaintext" in resp
            assert "CiphertextBlob" in resp
            assert "KeyId" in resp
            assert len(resp["Plaintext"]) == 32  # AES_256 = 32 bytes
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_generate_data_key_without_plaintext(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            resp = kms.generate_data_key_without_plaintext(
                KeyId=key_id, KeySpec="AES_256"
            )
            assert "CiphertextBlob" in resp
            assert "Plaintext" not in resp or resp.get("Plaintext") is None
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_generate_random(self, kms):
        resp = kms.generate_random(NumberOfBytes=32)
        assert "Plaintext" in resp
        assert len(resp["Plaintext"]) == 32

    def test_list_keys(self, kms):
        resp = kms.list_keys()
        assert "Keys" in resp
        assert isinstance(resp["Keys"], list)

    def test_list_aliases(self, kms):
        resp = kms.list_aliases()
        assert "Aliases" in resp

    def test_update_key_description(self, kms):
        key = kms.create_key(Description="original desc")
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            kms.update_key_description(KeyId=key_id, Description="updated desc")
            desc = kms.describe_key(KeyId=key_id)
            assert desc["KeyMetadata"]["Description"] == "updated desc"
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_enable_disable_key(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            kms.disable_key(KeyId=key_id)
            desc = kms.describe_key(KeyId=key_id)
            assert desc["KeyMetadata"]["Enabled"] is False

            kms.enable_key(KeyId=key_id)
            desc = kms.describe_key(KeyId=key_id)
            assert desc["KeyMetadata"]["Enabled"] is True
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_list_key_policies(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            resp = kms.list_key_policies(KeyId=key_id)
            assert "PolicyNames" in resp
            assert "default" in resp["PolicyNames"]
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_create_grant(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            resp = kms.create_grant(
                KeyId=key_id,
                GranteePrincipal="arn:aws:iam::123456789012:root",
                Operations=["Encrypt", "Decrypt"],
            )
            assert "GrantId" in resp
            assert "GrantToken" in resp

            grants = kms.list_grants(KeyId=key_id)
            grant_ids = [g["GrantId"] for g in grants["Grants"]]
            assert resp["GrantId"] in grant_ids

            kms.revoke_grant(KeyId=key_id, GrantId=resp["GrantId"])
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_encrypt_decrypt_roundtrip(self, kms):
        key = kms.create_key()
        key_id = key["KeyMetadata"]["KeyId"]
        try:
            plaintext = b"Hello KMS World!"
            enc_resp = kms.encrypt(KeyId=key_id, Plaintext=plaintext)
            assert "CiphertextBlob" in enc_resp

            dec_resp = kms.decrypt(CiphertextBlob=enc_resp["CiphertextBlob"])
            assert dec_resp["Plaintext"] == plaintext
        finally:
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

    def test_re_encrypt(self, kms):
        key1 = kms.create_key()
        key2 = kms.create_key()
        key1_id = key1["KeyMetadata"]["KeyId"]
        key2_id = key2["KeyMetadata"]["KeyId"]
        try:
            enc = kms.encrypt(KeyId=key1_id, Plaintext=b"re-encrypt test")
            re_enc = kms.re_encrypt(
                CiphertextBlob=enc["CiphertextBlob"],
                DestinationKeyId=key2_id,
            )
            assert "CiphertextBlob" in re_enc

            dec = kms.decrypt(CiphertextBlob=re_enc["CiphertextBlob"])
            assert dec["Plaintext"] == b"re-encrypt test"
        finally:
            kms.schedule_key_deletion(KeyId=key1_id, PendingWindowInDays=7)
            kms.schedule_key_deletion(KeyId=key2_id, PendingWindowInDays=7)
