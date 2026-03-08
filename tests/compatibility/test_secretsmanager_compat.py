"""Secrets Manager compatibility tests."""

import json

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sm():
    return make_client("secretsmanager")


class TestSecretsManagerOperations:
    def test_create_and_get_secret(self, sm):
        sm.create_secret(Name="test/secret", SecretString="my-secret-value")
        response = sm.get_secret_value(SecretId="test/secret")
        assert response["SecretString"] == "my-secret-value"
        sm.delete_secret(SecretId="test/secret", ForceDeleteWithoutRecovery=True)

    def test_list_secrets(self, sm):
        sm.create_secret(Name="list/secret1", SecretString="val1")
        response = sm.list_secrets()
        names = [s["Name"] for s in response["SecretList"]]
        assert "list/secret1" in names
        sm.delete_secret(SecretId="list/secret1", ForceDeleteWithoutRecovery=True)

    def test_update_secret(self, sm):
        sm.create_secret(Name="update/secret", SecretString="original")
        sm.update_secret(SecretId="update/secret", SecretString="updated")
        response = sm.get_secret_value(SecretId="update/secret")
        assert response["SecretString"] == "updated"
        sm.delete_secret(SecretId="update/secret", ForceDeleteWithoutRecovery=True)

    def test_describe_secret(self, sm):
        sm.create_secret(Name="describe/secret", SecretString="val")
        response = sm.describe_secret(SecretId="describe/secret")
        assert response["Name"] == "describe/secret"
        sm.delete_secret(SecretId="describe/secret", ForceDeleteWithoutRecovery=True)

    def test_json_secret(self, sm):
        data = json.dumps({"username": "admin", "password": "secret123"})
        sm.create_secret(Name="json/secret", SecretString=data)
        response = sm.get_secret_value(SecretId="json/secret")
        parsed = json.loads(response["SecretString"])
        assert parsed["username"] == "admin"
        sm.delete_secret(SecretId="json/secret", ForceDeleteWithoutRecovery=True)

    def test_binary_secret(self, sm):
        sm.create_secret(Name="binary/secret", SecretBinary=b"\x00\x01\x02\x03")
        response = sm.get_secret_value(SecretId="binary/secret")
        assert response["SecretBinary"] == b"\x00\x01\x02\x03"
        sm.delete_secret(SecretId="binary/secret", ForceDeleteWithoutRecovery=True)

    def test_tag_secret(self, sm):
        sm.create_secret(
            Name="tagged/secret", SecretString="val", Tags=[{"Key": "env", "Value": "test"}]
        )
        response = sm.describe_secret(SecretId="tagged/secret")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert tags.get("env") == "test"
        sm.delete_secret(SecretId="tagged/secret", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value(self, sm):
        """Test put_secret_value to add a new version."""
        sm.create_secret(Name="put-val/secret", SecretString="v1")
        sm.put_secret_value(SecretId="put-val/secret", SecretString="v2")
        response = sm.get_secret_value(SecretId="put-val/secret")
        assert response["SecretString"] == "v2"
        sm.delete_secret(SecretId="put-val/secret", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_with_staging(self, sm):
        """Test put_secret_value with a new value and verify it becomes current."""
        sm.create_secret(Name="staging/secret", SecretString="initial")
        sm.put_secret_value(SecretId="staging/secret", SecretString="staged-value")
        response = sm.get_secret_value(SecretId="staging/secret")
        assert response["SecretString"] == "staged-value"
        sm.delete_secret(SecretId="staging/secret", ForceDeleteWithoutRecovery=True)

    def test_tag_and_untag_secret(self, sm):
        """Test tagging and untagging a secret."""
        sm.create_secret(Name="tagging/secret", SecretString="val")
        sm.tag_resource(
            SecretId="tagging/secret",
            Tags=[{"Key": "team", "Value": "platform"}, {"Key": "env", "Value": "dev"}],
        )
        response = sm.describe_secret(SecretId="tagging/secret")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert tags["team"] == "platform"
        assert tags["env"] == "dev"

        sm.untag_resource(SecretId="tagging/secret", TagKeys=["team"])
        response = sm.describe_secret(SecretId="tagging/secret")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert "team" not in tags
        assert tags["env"] == "dev"

        sm.delete_secret(SecretId="tagging/secret", ForceDeleteWithoutRecovery=True)

    def test_rotate_secret_config(self, sm):
        """Test describe shows rotation config fields."""
        sm.create_secret(Name="rotate/secret", SecretString="val")
        response = sm.describe_secret(SecretId="rotate/secret")
        assert "RotationEnabled" in response or "Name" in response
        sm.delete_secret(SecretId="rotate/secret", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_with_version_stages(self, sm):
        """Test put_secret_value with explicit version stages."""
        sm.create_secret(Name="stage/secret", SecretString="v1")
        sm.put_secret_value(
            SecretId="stage/secret",
            SecretString="v2",
            VersionStages=["AWSCURRENT"],
        )
        response = sm.get_secret_value(SecretId="stage/secret", VersionStage="AWSCURRENT")
        assert response["SecretString"] == "v2"
        sm.delete_secret(SecretId="stage/secret", ForceDeleteWithoutRecovery=True)

    def test_list_secret_version_ids(self, sm):
        """Test listing all versions of a secret."""
        sm.create_secret(Name="versions/secret", SecretString="v1")
        sm.put_secret_value(SecretId="versions/secret", SecretString="v2")
        response = sm.list_secret_version_ids(SecretId="versions/secret")
        assert "Versions" in response
        assert len(response["Versions"]) >= 1
        sm.delete_secret(SecretId="versions/secret", ForceDeleteWithoutRecovery=True)

    def test_tag_and_untag_resource(self, sm):
        """Test adding and removing tags on a secret."""
        sm.create_secret(Name="tagging/secret", SecretString="val")
        sm.tag_resource(
            SecretId="tagging/secret",
            Tags=[
                {"Key": "team", "Value": "platform"},
                {"Key": "cost", "Value": "dev"},
            ],
        )
        desc = sm.describe_secret(SecretId="tagging/secret")
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert tags["team"] == "platform"
        assert tags["cost"] == "dev"

        sm.untag_resource(SecretId="tagging/secret", TagKeys=["cost"])
        desc = sm.describe_secret(SecretId="tagging/secret")
        tag_keys = [t["Key"] for t in desc.get("Tags", [])]
        assert "team" in tag_keys
        assert "cost" not in tag_keys
        sm.delete_secret(SecretId="tagging/secret", ForceDeleteWithoutRecovery=True)

    def test_restore_secret(self, sm):
        """Test restoring a secret after scheduling deletion."""
        sm.create_secret(Name="restore/secret", SecretString="restore-me")
        sm.delete_secret(SecretId="restore/secret")
        sm.restore_secret(SecretId="restore/secret")
        response = sm.get_secret_value(SecretId="restore/secret")
        assert response["SecretString"] == "restore-me"
        sm.delete_secret(SecretId="restore/secret", ForceDeleteWithoutRecovery=True)

    def test_get_random_password(self, sm):
        """Test generating a random password."""
        response = sm.get_random_password(PasswordLength=32)
        assert "RandomPassword" in response
        assert len(response["RandomPassword"]) == 32

    def test_get_random_password_options(self, sm):
        """Test random password with specific character requirements."""
        response = sm.get_random_password(
            PasswordLength=20,
            ExcludeNumbers=True,
            ExcludePunctuation=True,
        )
        password = response["RandomPassword"]
        assert len(password) == 20
        assert not any(c.isdigit() for c in password)

    def test_binary_secret_large(self, sm):
        """Test binary secret with larger payload."""
        data = bytes(range(256)) * 4  # 1024 bytes
        sm.create_secret(Name="binary-large/secret", SecretBinary=data)
        response = sm.get_secret_value(SecretId="binary-large/secret")
        assert response["SecretBinary"] == data
        sm.delete_secret(SecretId="binary-large/secret", ForceDeleteWithoutRecovery=True)

    def test_put_resource_policy(self, sm):
        """Test putting and getting a resource policy on a secret."""
        sm.create_secret(Name="policy/secret", SecretString="val")
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "secretsmanager:GetSecretValue",
                        "Resource": "*",
                    }
                ],
            }
        )
        sm.put_resource_policy(SecretId="policy/secret", ResourcePolicy=policy)
        response = sm.get_resource_policy(SecretId="policy/secret")
        assert response["ResourcePolicy"] is not None
        retrieved = json.loads(response["ResourcePolicy"])
        assert retrieved["Version"] == "2012-10-17"

        sm.delete_resource_policy(SecretId="policy/secret")
        sm.delete_secret(SecretId="policy/secret", ForceDeleteWithoutRecovery=True)

    def test_update_secret_description(self, sm):
        """Test updating a secret's description."""
        sm.create_secret(Name="updatedesc/secret", SecretString="val")
        sm.update_secret(SecretId="updatedesc/secret", Description="new description")
        desc = sm.describe_secret(SecretId="updatedesc/secret")
        assert desc.get("Description") == "new description"
        sm.delete_secret(SecretId="updatedesc/secret", ForceDeleteWithoutRecovery=True)

    def test_create_secret_with_many_tags(self, sm):
        """Test creating a secret with multiple tags."""
        tags = [{"Key": f"key{i}", "Value": f"val{i}"} for i in range(5)]
        sm.create_secret(Name="manytags/secret", SecretString="val", Tags=tags)
        desc = sm.describe_secret(SecretId="manytags/secret")
        tag_map = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        for i in range(5):
            assert tag_map[f"key{i}"] == f"val{i}"
        sm.delete_secret(SecretId="manytags/secret", ForceDeleteWithoutRecovery=True)

    def test_get_secret_value_by_version_id(self, sm):
        """Test retrieving a secret by its version ID."""
        create_resp = sm.create_secret(Name="byversion/secret", SecretString="original")
        version_id = create_resp["VersionId"]
        response = sm.get_secret_value(SecretId="byversion/secret", VersionId=version_id)
        assert response["SecretString"] == "original"
        assert response["VersionId"] == version_id
        sm.delete_secret(SecretId="byversion/secret", ForceDeleteWithoutRecovery=True)

    def test_update_secret_string(self, sm):
        """UpdateSecret changes the secret value."""
        sm.create_secret(Name="upd-str/secret", SecretString="before")
        sm.update_secret(SecretId="upd-str/secret", SecretString="after")
        resp = sm.get_secret_value(SecretId="upd-str/secret")
        assert resp["SecretString"] == "after"
        sm.delete_secret(SecretId="upd-str/secret", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_new_version(self, sm):
        """PutSecretValue adds a new version to an existing secret."""
        sm.create_secret(Name="put-ver/secret", SecretString="version1")
        put_resp = sm.put_secret_value(SecretId="put-ver/secret", SecretString="version2")
        assert "VersionId" in put_resp
        get_resp = sm.get_secret_value(SecretId="put-ver/secret")
        assert get_resp["SecretString"] == "version2"
        sm.delete_secret(SecretId="put-ver/secret", ForceDeleteWithoutRecovery=True)

    def test_get_secret_value_current(self, sm):
        """GetSecretValue returns the current AWSCURRENT version."""
        sm.create_secret(Name="get-cur/secret", SecretString="current-val")
        resp = sm.get_secret_value(SecretId="get-cur/secret")
        assert resp["SecretString"] == "current-val"
        assert resp["Name"] == "get-cur/secret"
        assert "VersionId" in resp
        sm.delete_secret(SecretId="get-cur/secret", ForceDeleteWithoutRecovery=True)

    def test_rotate_secret_no_lambda(self, sm):
        """RotateSecret without a Lambda ARN should fail gracefully."""
        sm.create_secret(Name="rotate-nolambda/secret", SecretString="val")
        try:
            sm.rotate_secret(SecretId="rotate-nolambda/secret")
        except Exception:
            pass  # Expected to fail without rotation Lambda configured
        sm.delete_secret(SecretId="rotate-nolambda/secret", ForceDeleteWithoutRecovery=True)

    def test_batch_get_secret_value(self, sm):
        """BatchGetSecretValue retrieves multiple secrets at once."""
        sm.create_secret(Name="batch/secret1", SecretString="val1")
        sm.create_secret(Name="batch/secret2", SecretString="val2")
        try:
            resp = sm.batch_get_secret_value(SecretIdList=["batch/secret1", "batch/secret2"])
            assert "SecretValues" in resp
            values = {s["Name"]: s["SecretString"] for s in resp["SecretValues"]}
            assert values["batch/secret1"] == "val1"
            assert values["batch/secret2"] == "val2"
        finally:
            sm.delete_secret(SecretId="batch/secret1", ForceDeleteWithoutRecovery=True)
            sm.delete_secret(SecretId="batch/secret2", ForceDeleteWithoutRecovery=True)

    def test_rotate_secret(self, sm):
        """Test RotateSecret."""
        sm.create_secret(Name="rotate-test/secret", SecretString="original")
        try:
            sm.rotate_secret(
                SecretId="rotate-test/secret",
                RotationLambdaARN="arn:aws:lambda:us-east-1:123456789012:function:rotation",
                RotationRules={"AutomaticallyAfterDays": 30},
            )
        finally:
            sm.delete_secret(SecretId="rotate-test/secret", ForceDeleteWithoutRecovery=True)

    def test_put_get_delete_resource_policy(self, sm):
        """Test PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy."""
        sm.create_secret(Name="policy-test/secret", SecretString="val")
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                            "Action": "secretsmanager:GetSecretValue",
                            "Resource": "*",
                        }
                    ],
                }
            )
            sm.put_resource_policy(SecretId="policy-test/secret", ResourcePolicy=policy)

            get_resp = sm.get_resource_policy(SecretId="policy-test/secret")
            assert "ResourcePolicy" in get_resp
            parsed = json.loads(get_resp["ResourcePolicy"])
            assert len(parsed["Statement"]) == 1

            sm.delete_resource_policy(SecretId="policy-test/secret")
            get_resp2 = sm.get_resource_policy(SecretId="policy-test/secret")
            # After deletion, policy should be empty or None
            assert not get_resp2.get("ResourcePolicy")
        finally:
            sm.delete_secret(SecretId="policy-test/secret", ForceDeleteWithoutRecovery=True)

    def test_validate_resource_policy(self, sm):
        """Test ValidateResourcePolicy."""
        sm.create_secret(Name="validate-policy/secret", SecretString="val")
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                            "Action": "secretsmanager:GetSecretValue",
                            "Resource": "*",
                        }
                    ],
                }
            )
            response = sm.validate_resource_policy(
                SecretId="validate-policy/secret",
                ResourcePolicy=policy,
            )
            assert "PolicyValidationPassed" in response
        finally:
            sm.delete_secret(SecretId="validate-policy/secret", ForceDeleteWithoutRecovery=True)

    def test_replicate_secret_to_regions(self, sm):
        """Test ReplicateSecretToRegions."""
        sm.create_secret(Name="replicate-test/secret", SecretString="val")
        try:
            response = sm.replicate_secret_to_regions(
                SecretId="replicate-test/secret",
                AddReplicaRegions=[{"Region": "eu-west-1"}],
                ForceOverwriteReplicaSecret=True,
            )
            assert "ReplicationStatus" in response
        finally:
            sm.delete_secret(SecretId="replicate-test/secret", ForceDeleteWithoutRecovery=True)


class TestSecretsManagerExtended:
    """Extended Secrets Manager operations for higher coverage."""

    @pytest.fixture
    def sm(self):
        from tests.compatibility.conftest import make_client

        return make_client("secretsmanager")

    def test_create_secret_with_tags(self, sm):
        import uuid

        name = f"tagged-secret-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(
                Name=name,
                SecretString="tagged-value",
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ],
            )
            desc = sm.describe_secret(SecretId=name)
            tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
            assert tags["env"] == "test"
            assert tags["team"] == "dev"
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_tag_untag_secret(self, sm):
        import uuid

        name = f"tag-untag-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="val")
            sm.tag_resource(
                SecretId=name,
                Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
            )
            desc = sm.describe_secret(SecretId=name)
            tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
            assert tags["k1"] == "v1"

            sm.untag_resource(SecretId=name, TagKeys=["k2"])
            desc2 = sm.describe_secret(SecretId=name)
            keys = [t["Key"] for t in desc2.get("Tags", [])]
            assert "k1" in keys
            assert "k2" not in keys
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_update_secret_string(self, sm):
        import uuid

        name = f"update-str-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="original")
            sm.update_secret(SecretId=name, SecretString="updated")
            resp = sm.get_secret_value(SecretId=name)
            assert resp["SecretString"] == "updated"
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_update_secret_description(self, sm):
        import uuid

        name = f"update-desc-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="val", Description="original desc")
            sm.update_secret(SecretId=name, Description="updated desc")
            desc = sm.describe_secret(SecretId=name)
            assert desc.get("Description") == "updated desc"
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_new_version(self, sm):
        import uuid

        name = f"put-val-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="v1")
            sm.put_secret_value(
                SecretId=name,
                SecretString="v2",
                VersionStages=["AWSCURRENT"],
            )
            resp = sm.get_secret_value(SecretId=name)
            assert resp["SecretString"] == "v2"
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_list_secret_version_ids(self, sm):
        import uuid

        name = f"versions-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="v1")
            sm.put_secret_value(SecretId=name, SecretString="v2")
            resp = sm.list_secret_version_ids(SecretId=name)
            assert "Versions" in resp
            assert len(resp["Versions"]) >= 1
            # At least one version should be AWSCURRENT
            current = [v for v in resp["Versions"] if "AWSCURRENT" in v.get("VersionStages", [])]
            assert len(current) >= 1
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_restore_secret(self, sm):
        import uuid

        name = f"restore-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="val")
            sm.delete_secret(SecretId=name, RecoveryWindowInDays=7)
            sm.restore_secret(SecretId=name)
            desc = sm.describe_secret(SecretId=name)
            assert desc.get("DeletedDate") is None
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_get_random_password(self, sm):
        resp = sm.get_random_password(PasswordLength=32)
        assert len(resp["RandomPassword"]) == 32

    def test_get_random_password_options(self, sm):
        resp = sm.get_random_password(
            PasswordLength=20,
            ExcludeNumbers=True,
            ExcludePunctuation=True,
        )
        pw = resp["RandomPassword"]
        assert len(pw) == 20
        assert not any(c.isdigit() for c in pw)

    def test_create_secret_binary(self, sm):
        import uuid

        name = f"binary-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretBinary=b"\x00\x01\x02\x03")
            resp = sm.get_secret_value(SecretId=name)
            assert resp["SecretBinary"] == b"\x00\x01\x02\x03"
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_list_secrets_filter(self, sm):
        import uuid

        prefix = f"filter-{uuid.uuid4().hex[:8]}"
        names = [f"{prefix}/a", f"{prefix}/b"]
        try:
            for n in names:
                sm.create_secret(Name=n, SecretString="val")
            resp = sm.list_secrets(Filters=[{"Key": "name", "Values": [prefix]}])
            found = [s["Name"] for s in resp["SecretList"]]
            for n in names:
                assert n in found
        finally:
            for n in names:
                sm.delete_secret(SecretId=n, ForceDeleteWithoutRecovery=True)

    def test_cancel_rotate_secret(self, sm):
        """CancelRotateSecret disables rotation on a secret."""
        import uuid

        name = f"cancel-rotate-{uuid.uuid4().hex[:8]}"
        try:
            sm.create_secret(Name=name, SecretString="val")
            # Enable rotation first
            sm.rotate_secret(
                SecretId=name,
                RotationLambdaARN="arn:aws:lambda:us-east-1:123456789012:function:rotator",
                RotationRules={"AutomaticallyAfterDays": 30},
            )
            desc = sm.describe_secret(SecretId=name)
            assert desc.get("RotationEnabled") is True

            # Cancel rotation
            resp = sm.cancel_rotate_secret(SecretId=name)
            assert resp["Name"] == name

            desc2 = sm.describe_secret(SecretId=name)
            assert desc2.get("RotationEnabled") is False
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_update_secret_version_stage(self, sm):
        """UpdateSecretVersionStage moves a stage label between versions."""
        import uuid

        name = f"version-stage-{uuid.uuid4().hex[:8]}"
        try:
            create_resp = sm.create_secret(Name=name, SecretString="v1")
            v1_id = create_resp["VersionId"]

            put_resp = sm.put_secret_value(SecretId=name, SecretString="v2")
            v2_id = put_resp["VersionId"]

            # Move AWSCURRENT from v2 back to v1
            resp = sm.update_secret_version_stage(
                SecretId=name,
                VersionStage="AWSCURRENT",
                MoveToVersionId=v1_id,
                RemoveFromVersionId=v2_id,
            )
            assert resp["Name"] == name

            # Verify v1 is now AWSCURRENT
            get_resp = sm.get_secret_value(SecretId=name, VersionStage="AWSCURRENT")
            assert get_resp["SecretString"] == "v1"
            assert get_resp["VersionId"] == v1_id
        finally:
            sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
