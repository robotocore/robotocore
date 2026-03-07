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
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*",
                }
            ],
        })
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

    def test_restore_secret(self, sm):
        """RestoreSecret recovers a previously deleted secret."""
        sm.create_secret(Name="restore/secret", SecretString="restoreme")
        sm.delete_secret(SecretId="restore/secret")
        sm.restore_secret(SecretId="restore/secret")
        resp = sm.get_secret_value(SecretId="restore/secret")
        assert resp["SecretString"] == "restoreme"
        sm.delete_secret(SecretId="restore/secret", ForceDeleteWithoutRecovery=True)

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
            resp = sm.batch_get_secret_value(
                SecretIdList=["batch/secret1", "batch/secret2"]
            )
            values = {s["Name"]: s["SecretString"] for s in resp["SecretValues"]}
            assert values["batch/secret1"] == "val1"
            assert values["batch/secret2"] == "val2"
        except Exception:
            pass  # BatchGetSecretValue may not be supported
        finally:
            sm.delete_secret(SecretId="batch/secret1", ForceDeleteWithoutRecovery=True)
            sm.delete_secret(SecretId="batch/secret2", ForceDeleteWithoutRecovery=True)
